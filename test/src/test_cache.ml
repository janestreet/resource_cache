open! Core
open! Async
open Expect_test_helpers

module Resource = struct
  module Key = Int
  module Common_args = Unit

  type t =
    { mutable status : [`Open | `Closed]
    ; close_finished : unit Ivar.t
    ; key : int
    ; id : int
    }
  [@@deriving fields]

  let counter = ref 0

  let open_ key () =
    let id = !counter in
    counter := !counter + 1;
    printf "Opening %d,%d\n" key id;
    Deferred.Or_error.return { status = `Open; close_finished = Ivar.create (); key; id }
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
      Ivar.fill t.close_finished ();
      Deferred.unit)
  ;;

  let close_finished t = Ivar.read t.close_finished
end

module Test_cache = struct
  include Resource_cache.Make (Resource)

  let init ~config k =
    init ~config ~log_error:(Log.Global.string ~level:`Error) k
  ;;
end

let config =
  { Resource_cache.Config.max_resources = 2
  ; idle_cleanup_after = Time_ns.Span.day
  ; max_resources_per_id = 1
  ; max_resource_reuse = 2
  ; close_idle_resources_when_at_limit = false
  }
;;

let get_resource ?(give_up = Deferred.never ()) ?load_balance ?r_ivar ~f t args =
  let%map result =
    Test_cache.with_any ~give_up ?load_balance t args ~f:(fun r ->
      printf "Got resource %d,%d\n" (Resource.key r) (Resource.id r);
      Option.iter r_ivar ~f:(fun r_ivar -> Ivar.fill r_ivar r);
      f r)
  in
  result
;;

let r_ivar_or_create r_ivar =
  match r_ivar with
  | None -> Ivar.create ()
  | Some r_ivar -> r_ivar
;;

let assert_resource_available
      ?(release = Deferred.unit)
      ?r_ivar
      ?give_up
      ?load_balance
      t
      args
  =
  let f r =
    let%bind () = release in
    printf "Releasing resource %d,%d\n" (Resource.key r) (Resource.id r);
    Deferred.unit
  in
  let r_ivar = r_ivar_or_create r_ivar in
  let%bind _, () = get_resource ~r_ivar ?give_up ?load_balance ~f t args >>| ok_exn in
  Ivar.read r_ivar
;;

let assert_resource_available' ?r_ivar ?give_up ?load_balance ~f t args =
  let r_ivar = r_ivar_or_create r_ivar in
  let%bind res = get_resource ~r_ivar ?give_up ?load_balance ~f t args in
  let%map r = Ivar.read r_ivar in
  r, res
;;

let assert_resource_available_now = assert_resource_available ~give_up:Deferred.unit
let assert_resource_available_now' = assert_resource_available' ~give_up:Deferred.unit

let assert_no_resource_available ?give_up t args =
  match%map
    get_resource ?give_up ~f:(fun _ -> raise_s [%message "Not possible"]) t args
  with
  | Ok (_, ()) -> raise_s [%message "asserted [with_] would return an error"]
  | Error e -> printf !"Error getting %{sexp: int list}: %{sexp:Error.t}\n" args e
;;

let assert_no_resource_available_now =
  assert_no_resource_available ~give_up:Deferred.unit
;;

let close_and_flush t =
  printf "Closing cache\n";
  let%map () = Test_cache.close_and_flush t in
  printf "Closed cache\n"
;;

(* 1 resource is allowed per id. Make sure a second one is not created until the
   first one has been released. The second job will reuse the same resource and then it
   will be closed because ttl is 2. *)
let%expect_test "respect [max_resources_per_id]" =
  let t = Test_cache.init ~config () in
  let release_r = Ivar.create () in
  let r_ivar = Ivar.create () in
  don't_wait_for
    (assert_resource_available_now ~r_ivar ~release:(Ivar.read release_r) t [ 0 ]
     >>| ignore);
  let%bind (_ : Resource.t) = Ivar.read r_ivar in
  let%bind () = [%expect {|
      Opening 0,0
      Got resource 0,0
    |}] in
  let%bind () = assert_no_resource_available_now t [ 0 ] in
  let%bind () =
    [%expect {|
      Error getting (0): "Gave up waiting for resource"
    |}]
  in
  Ivar.fill release_r ();
  let%bind (_ : Resource.t) = assert_resource_available t [ 0 ] in
  let%bind () =
    [%expect
      {|
      Releasing resource 0,0
      Got resource 0,0
      Releasing resource 0,0
      Closing 0,0
    |}]
  in
  return ()
;;

(* 2 resources are allowed to be open at the same time. Ensure a third is only opened
   once one of the previous 2 is closed. *)
let%expect_test "respect [max_resources]" =
  let t = Test_cache.init ~config () in
  let%bind r0 = assert_resource_available_now t [ 0 ] in
  let%bind () =
    [%expect {|
      Opening 0,1
      Got resource 0,1
      Releasing resource 0,1 |}]
  in
  let%bind r1 = assert_resource_available_now t [ 1 ] in
  let%bind () =
    [%expect {|
      Opening 1,2
      Got resource 1,2
      Releasing resource 1,2 |}]
  in
  let%bind () = assert_no_resource_available_now t [ 2 ] in
  let%bind () =
    [%expect {|
      Error getting (2): "Gave up waiting for resource" |}]
  in
  let r2_ivar = Ivar.create () in
  don't_wait_for
    (let%map r2 = assert_resource_available t [ 2 ] in
     Ivar.fill r2_ivar r2);
  let%bind () = Resource.close r0 in
  let%bind r2 = Ivar.read r2_ivar in
  let%bind () = Resource.close r2 in
  let%bind () = Resource.close r1 in
  let%bind () =
    [%expect
      {|
      Closing 0,1
      Opening 2,3
      Got resource 2,3
      Releasing resource 2,3
      Closing 2,3
      Closing 1,2 |}]
  in
  return ()
;;

(* We should get resource 0 the first time, because [with_any] respects the order of the
   args list. We should get resource 1 the second time because resource 0 is not
   available *)
let%expect_test "[with_any] respects order" =
  let t = Test_cache.init ~config () in
  let release_r0 = Ivar.create () in
  let r_ivar = Ivar.create () in
  don't_wait_for
    (assert_resource_available_now ~r_ivar ~release:(Ivar.read release_r0) t [ 0; 1 ]
     >>| ignore);
  let%bind (_ : Resource.t) = Ivar.read r_ivar in
  let%bind () = [%expect {|
      Opening 0,4
      Got resource 0,4 |}] in
  let release_r1 = Ivar.create () in
  let r_ivar = Ivar.create () in
  don't_wait_for
    (assert_resource_available_now ~r_ivar ~release:(Ivar.read release_r1) t [ 0; 1 ]
     >>| ignore);
  let%bind (_ : Resource.t) = Ivar.read r_ivar in
  let%bind () = [%expect {|
      Opening 1,5
      Got resource 1,5 |}] in
  let%bind () = assert_no_resource_available_now t [ 0; 1 ] in
  let%bind () = [%expect {| Error getting (0 1): "Gave up waiting for resource" |}] in
  Ivar.fill release_r0 ();
  let%bind (_ : Resource.t) = assert_resource_available t [ 0; 1 ] in
  let%bind () =
    [%expect
      {|
      Releasing resource 0,4
      Got resource 0,4
      Releasing resource 0,4
      Closing 0,4 |}]
  in
  return ()
;;

let%expect_test "[f] raises" =
  let t = Test_cache.init ~config () in
  let%bind () =
    match%map
      Monitor.try_with (fun () ->
        assert_resource_available_now' ~f:(fun _ -> failwith "failure") t [ 0 ])
    with
    | Ok (_r, _res) -> failwith "exn should have been caught"
    | Error exn -> show_raise ~hide_positions:true (fun () -> raise exn)
  in
  let%bind () =
    [%expect
      {|
      Opening 0,6
      Got resource 0,6
      Closing 0,6
      (raised (monitor.ml.Error (Failure failure) ("<backtrace elided in test>"))) |}]
  in
  return ()
;;

let%expect_test "close_and_flush with nothing open" =
  let t = Test_cache.init ~config () in
  let%bind () = close_and_flush t in
  let%bind () = [%expect {|
      Closing cache
      Closed cache
    |}] in
  return ()
;;

let%expect_test "close_and_flush with empty resource list" =
  let t = Test_cache.init ~config () in
  let%bind () =
    Deferred.List.iter (List.init config.max_resource_reuse ~f:Fn.id) ~f:(fun _ ->
      let%map (_ : Resource.t) = assert_resource_available_now t [ 0 ] in
      ())
  in
  let%bind () =
    [%expect
      {|
      Opening 0,7
      Got resource 0,7
      Releasing resource 0,7
      Got resource 0,7
      Releasing resource 0,7
      Closing 0,7 |}]
  in
  let%bind () =
    match%map Clock_ns.with_timeout (Time_ns.Span.of_sec 1.) (close_and_flush t) with
    | `Timeout -> printf "BUG: TIMEOUT\n"
    | `Result () -> ()
  in
  let%bind () = [%expect {|
      Closing cache
      Closed cache
    |}] in
  return ()
;;

(* [close_and_flush] should close all open resources and not allow subsequent calls to
   open new resources *)
let%expect_test "close_and_flush closes resources" =
  let t = Test_cache.init ~config () in
  let%bind (_ : Resource.t) = assert_resource_available_now t [ 0 ] in
  let%bind () =
    [%expect {|
      Opening 0,8
      Got resource 0,8
      Releasing resource 0,8 |}]
  in
  let%bind () = close_and_flush t in
  let%bind () =
    [%expect {|
      Closing cache
      Closing 0,8
      Closed cache |}]
  in
  assert (Test_cache.close_started t);
  let%bind () = assert_no_resource_available_now t [ 0 ] in
  let%bind () = [%expect {|
      Error getting (0): "Cache is closed" |}] in
  let%bind () = assert_no_resource_available t [ 0 ] in
  let%bind () = [%expect {|
      Error getting (0): "Cache is closed" |}] in
  return ()
;;

(* [close_and_flush] causes all jobs that are waiting for resources to return. The
   deferred returned by [close_and_flush] is only determined once all currently running
   jobs complete and all resources are closed. *)
let%expect_test "close_and_flush clears queue, waits for all jobs to finish" =
  let t = Test_cache.init ~config () in
  let release_r = Ivar.create () in
  let r_ivar = Ivar.create () in
  don't_wait_for
    (assert_resource_available_now ~release:(Ivar.read release_r) ~r_ivar t [ 0 ]
     >>| ignore);
  let%bind (_ : Resource.t) = Ivar.read r_ivar in
  let%bind () = [%expect {|
      Opening 0,9
      Got resource 0,9 |}] in
  let waiting_for_0 = assert_no_resource_available t [ 0 ] in
  let closed_and_flushed = close_and_flush t in
  assert (Test_cache.close_started t);
  let%bind () = waiting_for_0 in
  Ivar.fill release_r ();
  let%bind () = closed_and_flushed in
  let%bind () =
    [%expect
      {|
      Closing cache
      Error getting (0): "Cache is closed"
      Releasing resource 0,9
      Closing 0,9
      Closed cache |}]
  in
  return ()
;;

(* [with_any] and [max_resource_imbalance] should ensure that resources are distributed
   more evenly. *)
let test_imbalance ~load_balance =
  let t =
    Test_cache.init
      ~config:
        { max_resources = 10
        ; max_resources_per_id = 5
        ; max_resource_reuse = 1
        ; idle_cleanup_after = Time_ns.Span.day
        ; close_idle_resources_when_at_limit = false
        }
      ()
  in
  let%bind () =
    List.init 10 ~f:(fun (_ : int) ->
      assert_resource_available_now ~load_balance t [ 0; 1; 2; 3 ] >>| ignore)
    |> Deferred.all_unit
  in
  close_and_flush t
;;

let%expect_test "imbalanced resources" =
  let%bind () = test_imbalance ~load_balance:false in
  [%expect
    {|
    Opening 0,10
    Opening 0,11
    Opening 0,12
    Opening 0,13
    Opening 0,14
    Opening 1,15
    Opening 1,16
    Opening 1,17
    Opening 1,18
    Opening 1,19
    Got resource 0,10
    Got resource 0,11
    Got resource 0,12
    Got resource 0,13
    Got resource 0,14
    Got resource 1,15
    Got resource 1,16
    Got resource 1,17
    Got resource 1,18
    Got resource 1,19
    Releasing resource 0,10
    Releasing resource 0,11
    Releasing resource 0,12
    Releasing resource 0,13
    Releasing resource 0,14
    Releasing resource 1,15
    Releasing resource 1,16
    Releasing resource 1,17
    Releasing resource 1,18
    Releasing resource 1,19
    Closing 0,10
    Closing 0,11
    Closing 0,12
    Closing 0,13
    Closing 0,14
    Closing 1,15
    Closing 1,16
    Closing 1,17
    Closing 1,18
    Closing 1,19
    Closing cache
    Closed cache |}]
;;

let%expect_test "balanced resources" =
  let%bind () = test_imbalance ~load_balance:true in
  [%expect
    {|
    Opening 0,20
    Opening 1,21
    Opening 2,22
    Opening 3,23
    Opening 0,24
    Opening 1,25
    Opening 2,26
    Opening 3,27
    Opening 0,28
    Opening 1,29
    Got resource 0,20
    Got resource 1,21
    Got resource 2,22
    Got resource 3,23
    Got resource 0,24
    Got resource 1,25
    Got resource 2,26
    Got resource 3,27
    Got resource 0,28
    Got resource 1,29
    Releasing resource 0,20
    Releasing resource 1,21
    Releasing resource 2,22
    Releasing resource 3,23
    Releasing resource 0,24
    Releasing resource 1,25
    Releasing resource 2,26
    Releasing resource 3,27
    Releasing resource 0,28
    Releasing resource 1,29
    Closing 0,20
    Closing 1,21
    Closing 2,22
    Closing 3,23
    Closing 0,24
    Closing 1,25
    Closing 2,26
    Closing 3,27
    Closing 0,28
    Closing 1,29
    Closing cache
    Closed cache |}]
;;

module Open_resource = struct
  type t =
    { resource : Resource.t Deferred.t
    ; release : unit -> unit
    ; released : unit Deferred.t
    }

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
    let release () = Ivar.fill release_ivar () in
    { resource; release; released }
  ;;
end

let%expect_test "close idle resources when at limit" =
  Resource.counter := 0;
  let config =
    { Resource_cache.Config.max_resources = 2
    ; idle_cleanup_after = Time_ns.Span.day
    ; max_resources_per_id = 2
    ; max_resource_reuse = 2
    ; close_idle_resources_when_at_limit = true
    }
  in
  let t = Test_cache.init ~config () in
  let r0_0 = Open_resource.create ~now:true t [ 0 ] in
  let%bind r0_0_resource = r0_0.resource in
  let r0_1 = Open_resource.create ~now:true t [ 0 ] in
  let%bind r0_1_resource = r0_1.resource in
  r0_0.release ();
  let%bind () = r0_0.released in
  r0_1.release ();
  let%bind () = r0_1.released in
  (* There are 2 idle resources. [r0_0] is the least recently used. Trying to get a
     resource with key 1 results in closing [r0_0] to make room. *)
  let r1_0 = Open_resource.create t [ 1 ] in
  let%bind () = Resource.close_finished r0_0_resource in
  let%bind (_ : Resource.t) = r1_0.resource in
  (* Now we close [r0_1] to make room. *)
  let r1_1 = Open_resource.create t [ 1 ] in
  let%bind () = Resource.close_finished r0_1_resource in
  let%bind (_ : Resource.t) = r1_1.resource in
  let%bind () =
    [%expect
      {|
    Opening 0,0
    Got resource 0,0
    Opening 0,1
    Got resource 0,1
    Releasing resource 0,0
    Releasing resource 0,1
    Closing 0,0
    Opening 1,2
    Got resource 1,2
    Closing 0,1
    Opening 1,3
    Got resource 1,3 |}]
  in
  return ()
;;
