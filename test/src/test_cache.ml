open! Core
open! Async
open Expect_test_helpers_core

let () = Dynamic.set_root Backtrace.elide true

module Resource = struct
  module Key = Int

  module Common_args = struct
    type t = { block_open_until : unit Deferred.t }

    let default = { block_open_until = return () }
  end

  type t =
    { mutable status : [ `Open | `Closed ]
    ; close_finished : unit Ivar.t
    ; key : int
    ; id : int
    ; raise_now : unit Ivar.t
    }
  [@@deriving fields ~getters]

  let counter = ref 0

  let open_ key ({ block_open_until } : Common_args.t) =
    let raise_now = Ivar.create () in
    don't_wait_for
      (let%map () = Ivar.read raise_now in
       raise_s [%message "Raising as requested"]);
    let id = !counter in
    counter := !counter + 1;
    printf "Opening %d,%d\n" key id;
    let%bind () = block_open_until in
    Deferred.Or_error.return
      { status = `Open; close_finished = Ivar.create (); key; id; raise_now }
  ;;

  let has_close_started t =
    match t.status with
    | `Open -> false
    | `Closed -> true
  ;;

  let close t =
    if has_close_started t
    then Deferred.unit
    else (
      printf "Closing %d,%d\n" t.key t.id;
      t.status <- `Closed;
      Ivar.fill_exn t.close_finished ();
      Deferred.unit)
  ;;

  let close_finished t = Ivar.read t.close_finished
end

let sort_expect_test_output () =
  let output = Expect_test_helpers_core.expect_test_output () in
  List.iter
    ~f:print_endline
    (List.sort ~compare:String.compare (String.split_lines output))
;;

module Test_cache = struct
  include Resource_cache.Make (Resource) ()

  let init ~config k =
    Resource.counter := 0;
    init ~config ~log_error:(Log.Global.string ~level:`Error) k
  ;;

  let close_all_resources_for_key t key =
    let%map () = close_all_resources_for_key t key in
    sort_expect_test_output ()
  ;;
end

let get_resource ?(give_up = Deferred.never ()) ?r_ivar ~f t args =
  let%map result =
    Test_cache.with_any ~give_up t args ~f:(fun r ->
      printf "Got resource %d,%d\n" (Resource.key r) (Resource.id r);
      Option.iter r_ivar ~f:(fun r_ivar -> Ivar.fill_exn r_ivar r);
      f r)
  in
  result
;;

let r_ivar_or_create r_ivar =
  match r_ivar with
  | None -> Ivar.create ()
  | Some r_ivar -> r_ivar
;;

let assert_no_resource_available ?give_up t args =
  match%map
    get_resource
      ?give_up
      ~f:(fun _ ->
        raise_s [%message "Asserted no resource available, but got a resource"])
      t
      args
  with
  | Ok (_, ()) -> raise_s [%message "asserted [with_] would return an error"]
  | Error e -> printf !"Error getting %{sexp: int list}: %{sexp:Error.t}\n" args e
;;

let assert_no_resource_available_now = assert_no_resource_available ~give_up:Deferred.unit

module Open_resource : sig
  type t =
    { resource : Resource.t Deferred.t
    ; release : unit -> unit Deferred.t
    }

  val create : ?now:bool -> Test_cache.t -> int list -> t
end = struct
  type t =
    { resource : Resource.t Deferred.t
    ; release : unit -> unit Deferred.t
    }

  let assert_resource_available ?(release = Deferred.unit) ?r_ivar ?give_up t args =
    let f r =
      let%bind () = release in
      printf "Releasing resource %d,%d\n" (Resource.key r) (Resource.id r);
      Deferred.unit
    in
    let r_ivar = r_ivar_or_create r_ivar in
    let%bind _, () = get_resource ~r_ivar ?give_up ~f t args >>| ok_exn in
    Ivar.read r_ivar
  ;;

  let create ?(now = false) t ids =
    let f =
      if now
      then assert_resource_available ~give_up:Deferred.unit
      else assert_resource_available ?give_up:None
    in
    let resource_ivar = Ivar.create () in
    let release_ivar = Ivar.create () in
    let released =
      f ~r_ivar:resource_ivar ~release:(Ivar.read release_ivar) t ids
      >>| (ignore : Resource.t -> unit)
    in
    let resource = Ivar.read resource_ivar in
    let release () =
      Ivar.fill_exn release_ivar ();
      released
    in
    { resource; release }
  ;;
end

let close_and_flush t =
  printf "Closing cache\n";
  let closed_and_flushed =
    let%map () = Test_cache.close_and_flush t in
    printf "Closed cache\n"
  in
  match%map Clock_ns.with_timeout (Time_ns.Span.of_sec 1.) closed_and_flushed with
  | `Timeout -> printf "BUG: timeout waiting for [close_and_flush]\n"
  | `Result () -> ()
;;

let config =
  { Resource_cache.Config.max_resources = 2
  ; idle_cleanup_after = Time_ns.Span.day
  ; max_resources_per_id = 1
  ; max_resource_reuse = 2
  ; close_idle_resources_when_at_limit = false
  ; close_resource_on_unhandled_exn = true
  }
;;

let%expect_test "respect [max_resources_per_id]" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource. *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Only 1 resource is allowed per id. *)
  let%bind () = assert_no_resource_available_now t [ 0 ] in
  [%expect {| Error getting (0): "Gave up waiting for resource" |}];
  (* Release the original resource. *)
  let%bind () = r0.release () in
  [%expect {| Releasing resource 0,0 |}];
  (* Now we can reuse the released resource. *)
  let r0' = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0'.resource in
  [%expect {| Got resource 0,0 |}];
  (* Close on release because the [max_resource_reuse] is 2. *)
  let%bind () = r0'.release () in
  [%expect
    {|
    Releasing resource 0,0
    Closing 0,0
    |}];
  (* Close and flush with no resources open. *)
  let%bind () = close_and_flush t in
  [%expect
    {|
    Closing cache
    Closed cache
    |}];
  return ()
;;

let%expect_test "respect [max_resources]" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource and release it immediately. *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind r0_resource = r0.resource in
  let%bind () = r0.release () in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    Releasing resource 0,0
    |}];
  (* Open a resource with a different key and release it immediately. *)
  let r1 = Open_resource.create ~now:true t [ 1 ] in
  let%bind (_ : Resource.t) = r1.resource in
  let%bind () = r1.release () in
  [%expect
    {|
    Opening 1,1
    Got resource 1,1
    Releasing resource 1,1
    |}];
  (* We can't open a resource with a different key because [max_resources] is 2. *)
  let%bind () = assert_no_resource_available_now t [ 2 ] in
  [%expect {| Error getting (2): "Gave up waiting for resource" |}];
  (* Open a resource with a different key, without giving up. *)
  let r2 = Open_resource.create t [ 2 ] in
  (* Once we explicitly close a previous resource, we have capacity. *)
  let%bind () = Resource.close r0_resource in
  let%bind (_ : Resource.t) = r2.resource in
  [%expect
    {|
    Closing 0,0
    Opening 2,2
    Got resource 2,2
    |}];
  return ()
;;

let%expect_test "[with_any] respects order" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open any of [0; 1]. We should get 0 because [with_any] respects order. *)
  let r0 = Open_resource.create ~now:true t [ 0; 1 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Open any of [0; 1]. We should get a resource 1 because there is already a 0 resource
     in use (and the limit per key is 1). *)
  let r1 = Open_resource.create ~now:true t [ 0; 1 ] in
  let%bind (_ : Resource.t) = r1.resource in
  [%expect
    {|
    Opening 1,1
    Got resource 1,1
    |}];
  (* Now, neither of [0; 1] are available immediately. *)
  let%bind () = assert_no_resource_available_now t [ 0; 1 ] in
  [%expect {| Error getting (0 1): "Gave up waiting for resource" |}];
  (* Release resource 0. *)
  let%bind () = r0.release () in
  [%expect {| Releasing resource 0,0 |}];
  (* Make sure we can now get resource 0. *)
  let r0' = Open_resource.create ~now:true t [ 0; 1 ] in
  let%bind (_ : Resource.t) = r0'.resource in
  [%expect {| Got resource 0,0 |}];
  return ()
;;

let%expect_test "[f] raises" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource and pass through an [f] that raises. Make sure the resource is
     closed. *)
  let%bind () =
    match%map
      Deferred.Or_error.try_with_join ~run:`Schedule ~rest:`Log (fun () ->
        get_resource ~f:(fun _ -> failwith "failure") t [ 0 ])
    with
    | Ok (_r, _res) -> failwith "exn should have been caught"
    | Error e -> show_raise ~hide_positions:true (fun () -> Error.raise e)
  in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    Closing 0,0
    (raised (monitor.ml.Error (Failure failure) ("<backtrace elided in test>")))
    |}];
  return ()
;;

let%expect_test "[f] when it is enqueued raises to correct monitor and cleans up properly"
  =
  let t =
    Test_cache.init
      ~config:
        { max_resources = 1
        ; max_resources_per_id = 1
        ; max_resource_reuse = 10
        ; idle_cleanup_after = Time_ns.Span.day
        ; close_idle_resources_when_at_limit = false
        ; close_resource_on_unhandled_exn = false
        }
      Resource.Common_args.default
  in
  (* Open up a resource so we are triggering the case where we need to enqueue a [Job.t]
  *)
  let%bind r0 =
    match%map
      Monitor.try_with
        ~rest:(`Call (fun exn -> print_s [%message "unexpected exception" (exn : exn)]))
        (fun () ->
          let r0 = Open_resource.create ~now:true t [ 0 ] in
          let%map (_ : Resource.t) = r0.resource in
          r0)
    with
    | Ok r0 -> r0
    | Error exn -> raise exn
  in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  let deferred_result =
    Monitor.try_with ~name:"usage" (fun () ->
      get_resource ~f:(fun _ -> failwith "from within usage monitor") t [ 0 ])
  in
  (* Release the used resource so the above [get_resource] call grabs the available
     resource *)
  let%bind () = r0.release () in
  [%expect
    {|
    Releasing resource 0,0
    Got resource 0,0
    |}];
  let%bind () =
    match%map deferred_result with
    | Ok _ -> failwith "an exception was expected"
    | Error e -> show_raise ~hide_positions:true (fun () -> raise e)
  in
  (* We expect the exception to be raised to the [Monitor.try_with] monitor, which causes
     [deferred_result] to be determined with an error. If it raised to the wrong monitor,
     then [deferred_result] would never be determined. *)
  [%expect
    {|
    Closing 0,0
    (raised (
      monitor.ml.Error
      (Failure "from within usage monitor")
      ("<backtrace elided in test>")))
    |}];
  (* Because of the exception, the resource should be closed. There is now capacity for a
     new resource to be opened. *)
  let%bind () =
    match%map get_resource ~f:(fun _ -> return ()) t [ 0 ] with
    | Error error -> Error.raise error
    | Ok (_, ()) -> ()
  in
  [%expect
    {|
    Opening 0,1
    Got resource 0,1
    |}];
  return ()
;;

let%expect_test "close_and_flush with nothing open" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  let%bind () = close_and_flush t in
  [%expect
    {|
    Closing cache
    Closed cache
    |}];
  return ()
;;

let%expect_test "close_and_flush closes resources" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Release the resource *)
  let%bind () = r0.release () in
  [%expect {| Releasing resource 0,0 |}];
  (* [close_and_flush] should close the idle resource. *)
  let%bind () = close_and_flush t in
  [%expect
    {|
    Closing cache
    Closing 0,0
    Closed cache
    |}];
  assert (Test_cache.close_started t);
  let%bind () = assert_no_resource_available_now t [ 0 ] in
  [%expect {| Error getting (0): "Cache is closed" |}];
  let%bind () = assert_no_resource_available t [ 0 ] in
  [%expect {| Error getting (0): "Cache is closed" |}];
  return ()
;;

let%expect_test "close_and_flush clears queue, waits for all jobs to finish" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Start waiting for resource 0 *)
  let waiting_for_r0 = assert_no_resource_available t [ 0 ] in
  (* [close_and_flush] while there is a job waiting for a resource. *)
  let closed_and_flushed = close_and_flush t in
  assert (Test_cache.close_started t);
  (* The waiting job stops waiting. *)
  let%bind () = waiting_for_r0 in
  [%expect
    {|
    Closing cache
    Error getting (0): "Cache is closed"
    |}];
  (* Release the open resource. Now, [closed_and_flushed] will become determined. *)
  let%bind () = r0.release () in
  let%bind () = closed_and_flushed in
  [%expect
    {|
    Releasing resource 0,0
    Closing 0,0
    Closed cache
    |}];
  return ()
;;

let%expect_test "[keep_cache_warm]" =
  let config =
    { Resource_cache.Config.max_resources = 5
    ; idle_cleanup_after = Time_ns.Span.day
    ; max_resources_per_id = 2
    ; max_resource_reuse = 5
    ; close_idle_resources_when_at_limit = false
    ; close_resource_on_unhandled_exn = true
    }
  in
  let t = Test_cache.init ~config Resource.Common_args.default in
  let keep_cache_warm ~num_resources_to_keep_open_per_key =
    Test_cache.keep_cache_warm
      t
      ~on_error_opening_resource:(fun ~key ~error ->
        print_s [%message "Error opening resource" (key : int) (error : Error.t)])
      ~num_resources_to_keep_open_per_key
      [ 0; 1 ]
  in
  Expect_test_helpers_core.require_does_raise (fun () ->
    keep_cache_warm ~num_resources_to_keep_open_per_key:0);
  [%expect
    {|
    ("Invalid value for [num_resources_to_keep_open_per_key] in call to [Resource_cache.keep_cache_warm]: must be greater than 0, less than or equal to [config.max_resources_per_id], and [num_resources_to_keep_open_per_key * List.length keys] must be less than or equal to [config.max_resources]"
     (num_resources_to_keep_open_per_key 0)
     ("List.length keys"                 2)
     ("(t.config).max_resources_per_id"  2)
     ("(t.config).max_resources"         5))
    |}];
  Expect_test_helpers_core.require_does_raise (fun () ->
    keep_cache_warm ~num_resources_to_keep_open_per_key:3);
  [%expect
    {|
    ("Invalid value for [num_resources_to_keep_open_per_key] in call to [Resource_cache.keep_cache_warm]: must be greater than 0, less than or equal to [config.max_resources_per_id], and [num_resources_to_keep_open_per_key * List.length keys] must be less than or equal to [config.max_resources]"
     (num_resources_to_keep_open_per_key 3)
     ("List.length keys"                 2)
     ("(t.config).max_resources_per_id"  2)
     ("(t.config).max_resources"         5))
    |}];
  (* Warming the cache opens up 2 resources for each of keys 0 and 1 *)
  keep_cache_warm ~num_resources_to_keep_open_per_key:2;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    Opening 0,0
    Opening 0,1
    Opening 1,2
    Opening 1,3
    |}];
  (* Opening a resource will get one in the cache *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind () = r0.release () in
  [%expect
    {|
    Got resource 0,0
    Releasing resource 0,0
    |}];
  (* When we close the resource, the cache warming will open up another one *)
  let%bind r0_resource = r0.resource in
  let%bind () = Resource.close r0_resource in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    Closing 0,0
    Opening 0,4
    |}];
  return ()
;;

let%expect_test "[warm_cache]" =
  let config =
    { Resource_cache.Config.max_resources = 5
    ; idle_cleanup_after = Time_ns.Span.day
    ; max_resources_per_id = 2
    ; max_resource_reuse = 5
    ; close_idle_resources_when_at_limit = false
    ; close_resource_on_unhandled_exn = true
    }
  in
  let t = Test_cache.init ~config Resource.Common_args.default in
  let warm_cache ~num_resources_to_keep_open_per_key =
    Test_cache.warm_cache
      t
      ~on_error_opening_resource:(fun ~key ~error ->
        print_s [%message "Error opening resource" (key : int) (error : Error.t)])
      ~num_resources_to_keep_open_per_key
      [ 0; 1 ]
  in
  (* Test validation - same as keep_cache_warm *)
  Expect_test_helpers_core.require_does_raise (fun () ->
    warm_cache ~num_resources_to_keep_open_per_key:0 |> Deferred.ignore_m);
  [%expect
    {|
    ("Invalid value for [num_resources_to_keep_open_per_key] in call to [Resource_cache.warm_cache]: must be greater than 0, less than or equal to [config.max_resources_per_id], and [num_resources_to_keep_open_per_key * List.length keys] must be less than or equal to [config.max_resources]"
     (num_resources_to_keep_open_per_key 0)
     ("List.length keys"                 2)
     ("(t.config).max_resources_per_id"  2)
     ("(t.config).max_resources"         5))
    |}];
  Expect_test_helpers_core.require_does_raise (fun () ->
    warm_cache ~num_resources_to_keep_open_per_key:3 |> Deferred.ignore_m);
  [%expect
    {|
    ("Invalid value for [num_resources_to_keep_open_per_key] in call to [Resource_cache.warm_cache]: must be greater than 0, less than or equal to [config.max_resources_per_id], and [num_resources_to_keep_open_per_key * List.length keys] must be less than or equal to [config.max_resources]"
     (num_resources_to_keep_open_per_key 3)
     ("List.length keys"                 2)
     ("(t.config).max_resources_per_id"  2)
     ("(t.config).max_resources"         5))
    |}];
  (* Warming the cache opens up 2 resources for each of keys 0 and 1 *)
  let%bind () = warm_cache ~num_resources_to_keep_open_per_key:2 in
  [%expect
    {|
    Opening 0,0
    Opening 0,1
    Opening 1,2
    Opening 1,3
    |}];
  (* Opening a resource will get one in the cache *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind () = r0.release () in
  [%expect
    {|
    Got resource 0,0
    Releasing resource 0,0
    |}];
  (* Unlike keep_cache_warm, warm_cache doesn't run a loop, so closing a resource doesn't
     trigger opening a new one *)
  let%bind r0_resource = r0.resource in
  let%bind () = Resource.close r0_resource in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| Closing 0,0 |}];
  (* But we can call warm_cache again to re-warm *)
  let%bind () = warm_cache ~num_resources_to_keep_open_per_key:2 in
  [%expect {| Opening 0,4 |}];
  (* If we already have enough resources, warm_cache is a no-op *)
  let%bind () = warm_cache ~num_resources_to_keep_open_per_key:2 in
  [%expect {| |}];
  (* We can warm to a lower number - it won't close existing resources *)
  let%bind () = warm_cache ~num_resources_to_keep_open_per_key:1 in
  [%expect {| |}];
  return ()
;;

let%expect_test "close idle resources when at limit" =
  let config =
    { Resource_cache.Config.max_resources = 2
    ; idle_cleanup_after = Time_ns.Span.day
    ; max_resources_per_id = 2
    ; max_resource_reuse = 2
    ; close_idle_resources_when_at_limit = true
    ; close_resource_on_unhandled_exn = false
    }
  in
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource. *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind r0_resource = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Open another resource with the same key. *)
  let r0' = Open_resource.create ~now:true t [ 0 ] in
  let%bind r0'_resource = r0'.resource in
  [%expect
    {|
    Opening 0,1
    Got resource 0,1
    |}];
  (* Release the first resource. *)
  let%bind () = r0.release () in
  [%expect {| Releasing resource 0,0 |}];
  (* Release the second resource. *)
  let%bind () = r0'.release () in
  [%expect {| Releasing resource 0,1 |}];
  (* There are 2 idle resources. [r0] is the least recently used. Trying to get a resource
     with key 1 results in closing [r0] to make room. *)
  let r1 = Open_resource.create t [ 1 ] in
  let%bind () = Resource.close_finished r0_resource in
  let%bind (_ : Resource.t) = r1.resource in
  [%expect
    {|
    Closing 0,0
    Opening 1,2
    Got resource 1,2
    |}];
  (* Now we close [r0'] to make room. *)
  let r1' = Open_resource.create t [ 1 ] in
  let%bind () = Resource.close_finished r0'_resource in
  let%bind (_ : Resource.t) = r1'.resource in
  [%expect
    {|
    Closing 0,1
    Opening 1,3
    Got resource 1,3
    |}];
  return ()
;;

let%expect_test "raise after open" =
  Log.Global.set_output
    [ Log.For_testing.create_output ~map_output:(fun x ->
        Sexp.to_string_hum (Sexp.of_string x))
    ];
  let test ~close_resource_on_unhandled_exn =
    let config =
      { Resource_cache.Config.max_resources = 2
      ; idle_cleanup_after = Time_ns.Span.day
      ; max_resources_per_id = 2
      ; max_resource_reuse = 2
      ; close_idle_resources_when_at_limit = true
      ; close_resource_on_unhandled_exn
      }
    in
    let t = Test_cache.init ~config Resource.Common_args.default in
    Test_cache.with_ t 0 ~f:(fun r ->
      Ivar.fill_exn r.raise_now ();
      Log.flushed (force Log.Global.log))
    |> Deferred.Or_error.ok_exn
  in
  let%bind () = test ~close_resource_on_unhandled_exn:false in
  [%expect
    {|
    Opening 0,0
    ("Exception raised to [Monitor.try_with] that already returned."
     "This error was captured by a default handler in [Async.Log]."
     (exn
      (monitor.ml.Error "Raising as requested" ("<backtrace elided in test>"))))
    |}];
  let%bind () = test ~close_resource_on_unhandled_exn:true in
  [%expect
    {|
    Opening 0,0
    ("Exception raised to [Monitor.try_with] that already returned."
     "This error was captured by a default handler in [Async.Log]."
     (exn
      ("Resource raised exn after creation" (key 0)
       (monitor.ml.Error "Raising as requested" ("<backtrace elided in test>")))))
    Closing 0,0
    |}];
  return ()
;;

let%expect_test "regression test: don't hang if open_ is in progress when \
                 close_and_flush is called"
  =
  Log.Global.set_output
    [ Log.For_testing.create_output ~map_output:(fun x ->
        Sexp.to_string_hum (Sexp.of_string x))
    ];
  let block_open_until = Ivar.create () in
  let t = Test_cache.init ~config { block_open_until = Ivar.read block_open_until } in
  let (_ : unit Or_error.t Deferred.t) =
    Test_cache.with_ t 0 ~f:(fun r ->
      printf "Running with id %d" r.id;
      return ())
  in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| Opening 0,0 |}];
  let closing = close_and_flush t in
  [%expect {| Closing cache |}];
  Ivar.fill_exn block_open_until ();
  let%bind () = closing in
  [%expect {| Closed cache |}];
  return ()
;;

let%expect_test "close_all_resources_for_key - basic functionality" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open resources for keys 0 and 1 *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  let r1 = Open_resource.create ~now:true t [ 1 ] in
  let%bind (_ : Resource.t) = r1.resource in
  [%expect
    {|
    Opening 1,1
    Got resource 1,1
    |}];
  (* Release both resources *)
  let%bind () = r0.release () in
  [%expect {| Releasing resource 0,0 |}];
  let%bind () = r1.release () in
  [%expect {| Releasing resource 1,1 |}];
  (* Close all resources for key 0 *)
  let%bind () = Test_cache.close_all_resources_for_key t 0 in
  [%expect {| Closing 0,0 |}];
  (* Key 0 can be used again - it will open a new resource *)
  let r0' = Open_resource.create t [ 0 ] in
  let%bind (_ : Resource.t) = r0'.resource in
  [%expect
    {|
    Opening 0,2
    Got resource 0,2
    |}];
  (* Key 1 should still have its cached resource *)
  let r1' = Open_resource.create ~now:true t [ 1 ] in
  let%bind (_ : Resource.t) = r1'.resource in
  [%expect {| Got resource 1,1 |}];
  return ()
;;

let%expect_test "close_all_resources_for_key - non-existent key" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Closing resources for a non-existent key should be a no-op *)
  let%bind () = Test_cache.close_all_resources_for_key t 99 in
  [%expect {| |}];
  (* Cache should still work normally *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  return ()
;;

let%expect_test "close_all_resources_for_key - waits for busy resources" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource and keep it busy *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Start closing all resources for key 0 (should wait for r0 to be released) *)
  let closing = Test_cache.close_all_resources_for_key t 0 in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  (* The resource should not be closed yet *)
  let%bind () = r0.release () in
  [%expect
    {|
    Releasing resource 0,0
    Closing 0,0
    |}];
  (* Now closing should complete *)
  let%bind () = closing in
  [%expect {| |}];
  return ()
;;

let%expect_test "close_all_resources_for_key - multiple resources for same key" =
  let config =
    { Resource_cache.Config.max_resources = 5
    ; idle_cleanup_after = Time_ns.Span.day
    ; max_resources_per_id = 3
    ; max_resource_reuse = 10
    ; close_idle_resources_when_at_limit = false
    ; close_resource_on_unhandled_exn = true
    }
  in
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open multiple resources for key 0 *)
  let r0_1 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0_1.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  let r0_2 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0_2.resource in
  [%expect
    {|
    Opening 0,1
    Got resource 0,1
    |}];
  let r0_3 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0_3.resource in
  [%expect
    {|
    Opening 0,2
    Got resource 0,2
    |}];
  (* Release all resources *)
  let%bind () = r0_1.release () in
  [%expect {| Releasing resource 0,0 |}];
  let%bind () = r0_2.release () in
  [%expect {| Releasing resource 0,1 |}];
  let%bind () = r0_3.release () in
  [%expect {| Releasing resource 0,2 |}];
  (* Close all resources for key 0 - should close all three *)
  let%bind () = Test_cache.close_all_resources_for_key t 0 in
  [%expect
    {|
    Closing 0,0
    Closing 0,1
    Closing 0,2
    |}];
  return ()
;;

let%expect_test "close_all_resources_for_key - jobs waiting for resources" =
  let t = Test_cache.init ~config Resource.Common_args.default in
  (* Open a resource *)
  let r0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind (_ : Resource.t) = r0.resource in
  [%expect
    {|
    Opening 0,0
    Got resource 0,0
    |}];
  (* Start a job waiting for resource 0 *)
  let waiting_for_r0 = Open_resource.create ~now:false t [ 0 ] in
  (* Close all resources for key 0 *)
  let closing = Test_cache.close_all_resources_for_key t 0 in
  (* allow the scheduler to run *)
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  (* The resource is blocked until the previous resource gets released. *)
  print_s [%sexp (Deferred.is_determined waiting_for_r0.resource : bool)];
  [%expect {| false |}];
  (* Release the resource so closing can complete *)
  let%bind () = r0.release () in
  [%expect
    {|
    Releasing resource 0,0
    Closing 0,0
    |}];
  let%bind () = closing in
  [%expect {| Opening 0,1 |}];
  (* and now the resource we requested earlier will become available as expected. *)
  let%bind (_ : Resource.t) = waiting_for_r0.resource in
  [%expect {| Got resource 0,1 |}];
  return ()
;;

let%expect_test "keep_warm works with close_all_resources_for_key" =
  let t =
    Test_cache.init
      ~config:{ config with max_resources_per_id = 10 }
      Resource.Common_args.default
  in
  Test_cache.keep_cache_warm
    t
    ~on_error_opening_resource:(fun ~key ~error ->
      print_s [%message "Error opening resource" (key : int) (error : Error.t)])
    ~num_resources_to_keep_open_per_key:2
    [ 0 ];
  (* wait for the initial resources to get opened. *)
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    Opening 0,0
    Opening 0,1
    |}];
  (* close all the currently open resources *)
  let%bind () = Test_cache.close_all_resources_for_key t 0 in
  [%expect
    {|
    Closing 0,0
    Closing 0,1
    |}];
  (* wait for new resources to get opened. *)
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    Opening 0,2
    Opening 0,3
    |}];
  return ()
;;
