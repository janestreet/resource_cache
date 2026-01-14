module Stable = struct
  open! Core.Core_stable

  module Resource = struct
    module V1 = struct
      type t =
        { state : [ `Busy | `Closing | `Idle ]
        ; since : Time_ns.Span.V2.t
        }
      [@@deriving sexp, bin_io]
    end
  end

  module type Key = sig
    type t [@@deriving sexp, bin_io]
  end

  module Resource_list = struct
    module V1 = struct
      type 'key t =
        { key : 'key
        ; resources : Resource.V1.t list
        ; queue_length : int
        ; max_time_on_queue : Time_ns.Span.V2.t option
        }
      [@@deriving sexp, bin_io]
    end
  end

  module Status = struct
    module V1 = struct
      type 'key t =
        { resource_lists : 'key Resource_list.V1.t list
        ; num_jobs_in_cache : int
        }
      [@@deriving sexp, bin_io]
    end
  end
end

open! Core
open! Async_kernel
open! Import
include Resource_cache_intf

module Make_wrapped (R : Resource.S_wrapped) () = struct
  module Status = struct
    module Key = R.Key

    module Resource = struct
      type state =
        [ `Busy
        | `Idle
        | `Closing
        ]
      [@@deriving sexp_of, compare]

      type t = Stable.Resource.V1.t =
        { state : state
        ; since : Time_ns.Span.t
        }
      [@@deriving sexp_of, compare]
    end

    module Resource_list = struct
      type 'key t_ = 'key Stable.Resource_list.V1.t =
        { key : 'key
        ; resources : Resource.t list
        ; queue_length : int
        ; max_time_on_queue : Time_ns.Span.t option
        }
      [@@deriving fields ~getters, sexp_of, compare]

      type t = Key.t t_ [@@deriving sexp_of, compare]
    end

    type 'key t_ = 'key Stable.Status.V1.t =
      { resource_lists : 'key Resource_list.t_ list
      ; num_jobs_in_cache : int
      }
    [@@deriving fields ~getters, sexp_of, compare]

    type t = Key.t t_ [@@deriving sexp_of, compare]

    module Make_stable = struct
      module V1 (Key : Stable.Key with type t = Key.t) = struct
        type t = Key.t Stable.Status.V1.t [@@deriving sexp, bin_io]
      end
    end
  end

  module Delayed_failures = struct
    type t =
      [ `Error_opening_resource of R.Key.t * Error.t
      | `Cache_is_closed
      ]
  end

  module Job : sig
    type 'a t

    val create
      :  ?open_timeout:Time_ns.Span.t
      -> give_up:unit Deferred.t
      -> f:(R.t -> 'a Deferred.t)
      -> 'a t

    (* Use [has_result t] instead of [Deferred.is_determined (result t)] to prevent a race
       condition. It is possible that the result ivar was filled but [result] is not yet
       determined. *)

    val has_result : _ t -> bool

    val result
      :  'a t
      -> [ `Result of R.Key.t * ('a, exn) Result.t
         | `Gave_up_waiting_for_resource
         | Delayed_failures.t
         ]
           Deferred.t

    val f : 'a t -> R.t -> 'a Deferred.t
    val open_timeout : 'a t -> Time_ns.Span.t option
    val created_at : 'a t -> Time_ns.t

    val mark_result_from_available_resource
      :  'a t
      -> R.Key.t
      -> result:('a, exn) Deferred.Result.t
      -> unit

    val mark_result_from_resource_creation
      :  'a t
      -> result:
           [ `Result of R.Key.t * ('a, exn) Result.t
           | Delayed_failures.t
           | (* This case is not possible, but the compiler gets mad otherwise *)
             `Gave_up_waiting_for_resource
           ]
             Deferred.t
      -> unit

    val mark_cache_closed : 'a t -> unit
  end = struct
    type 'a t =
      { f : R.t -> 'a Deferred.t
      ; result_ivar :
          [ `Result of R.Key.t * ('a, exn) Result.t
          | `Gave_up_waiting_for_resource
          | Delayed_failures.t
          ]
            Deferred.t
            Ivar.t
      ; open_timeout : Time_ns.Span.t option
      ; created_at : Time_ns.t
      }
    [@@deriving fields ~getters]

    let[@warning "-16"] create ?open_timeout ~give_up ~f =
      let result_ivar = Ivar.create () in
      upon give_up (fun () ->
        Ivar.fill_if_empty result_ivar (return `Gave_up_waiting_for_resource));
      { f; result_ivar; open_timeout; created_at = Time_ns.now () }
    ;;

    let mark_result_from_available_resource t args ~result =
      Ivar.fill_exn
        t.result_ivar
        (let%map res = result in
         `Result (args, res))
    ;;

    let mark_result_from_resource_creation t ~result = Ivar.fill_exn t.result_ivar result
    let mark_cache_closed t = Ivar.fill_if_empty t.result_ivar (return `Cache_is_closed)
    let has_result t = Ivar.is_full t.result_ivar

    let result t =
      let%bind result = Ivar.read t.result_ivar in
      result
    ;;
  end

  (* [Resource] wraps [R] taking care that uses of [with_] don't cross paths, and that
     [close] and [close_finished] are well behaved.

     It will trigger [close] once the [max_resource_reuse] or [idle_cleanup_after] are
     exceeded. *)
  module Resource : sig
    module Id : Unique_id.Id

    module State : sig
      type t =
        [ `Idle
        | `In_use_until of unit Ivar.t
        | `Closing
        ]
    end

    type t

    val id : t -> Id.t

    (* [create] will immediately produce a [Resource.t] that is initially busy with:
       - calling [R.open_]
       - calling [immediate ~f:with_] with the resource created (if successful)

       If [R.open_] fails, this resource is immediately closed otherwise the resource will
       become idle after the initial use.

       \@see [immediate]. *)

    val create
      :  ?open_timeout:Time_ns.Span.t
      -> ?on_state_update:(t -> State.t -> unit)
      -> Config.t
      -> R.Key.t
      -> R.Common_args.t
      -> with_:(R.t -> 'a Deferred.t)
      -> log_error:(string -> unit)
      -> t * [> `Result of R.Key.t * ('a, exn) Result.t | Delayed_failures.t ] Deferred.t

    val status : t -> Status.Resource.t

    (* [close_when_idle] forces the resource to shutdown either now or when the currently
       running [f] completes *)

    val close_when_idle : t -> unit Deferred.t

    (* [close_finished] becomes determined when this [Resource] has been closed. We
       guarantee that this will become determined, even if the underlying resource
       implementation is not well behaved. *)

    val close_finished : t -> unit Deferred.t

    (* Aquire an exclusive lock on this resource and call [f]. If [f] fails, or if the
       number of calls exceeds [max_resource_reuse] this resource will be closed.
       Otherwise this resource will be marked as idle and will close if not used again
       within a predefined timeout. *)

    val immediate
      :  t
      -> f:(R.t -> 'a Deferred.t)
      -> [ `Result of ('a, exn) Deferred.Result.t
         | `Resource_unavailable_until of unit Deferred.t
         | `Resource_closed
         ]
  end = struct
    module Id = Unique_id.Int ()

    module State = struct
      type t =
        [ `Idle
        | `In_use_until of unit Ivar.t
        | `Closing
        ]
    end

    type t =
      { id : Id.t
      ; key : R.Key.t
      ; args : R.Common_args.t
      ; resource : R.t Set_once.t
      ; mutable state : State.t
      ; mutable in_state_since : Time_ns.t
      ; config : Config.t
      ; mutable remaining_uses : int
      ; close_finished : unit Ivar.t
      ; on_state_update : (t -> State.t -> unit) option
      ; log_error : string -> unit
      }

    let id t = t.id

    let status t =
      let state =
        match t.state with
        | `Idle -> `Idle
        | `In_use_until _ -> `Busy
        | `Closing -> `Closing
      in
      { Status.Resource.state; since = Time_ns.diff (Time_ns.now ()) t.in_state_since }
    ;;

    let set_state t state =
      Option.iter t.on_state_update ~f:(fun f -> f t state);
      t.state <- state;
      t.in_state_since <- Time_ns.now ()
    ;;

    let close_finished t = Ivar.read t.close_finished

    let close t =
      let really_close () =
        set_state t `Closing;
        let closed =
          match Set_once.get t.resource with
          | None -> Deferred.unit
          | Some r ->
            (match%map
               Monitor.try_with ~run:`Schedule ~rest:`Log (fun () ->
                 if R.has_close_started r then Deferred.unit else R.close r)
             with
             | Ok () -> ()
             | Error exn ->
               t.log_error (sprintf !"Exception closing resource: %{Exn}" exn))
        in
        match%map Clock_ns.with_timeout (Time_ns.Span.of_sec 10.) closed with
        | `Result () | `Timeout -> Ivar.fill_exn t.close_finished ()
      in
      match t.state with
      | `Closing -> close_finished t
      | `Idle -> really_close ()
      | `In_use_until done_ ->
        assert (not (Ivar.is_full done_));
        close_finished t >>> Ivar.fill_exn done_;
        really_close ()
    ;;

    let close_when_idle t =
      match t.state with
      | `Closing -> close_finished t
      | `Idle -> close t
      | `In_use_until _ ->
        (* This will trigger a [close] when the current task completes. *)
        t.remaining_uses <- 0;
        close_finished t
    ;;

    let set_idle t =
      match t.state with
      | `Closing -> failwith "Impossible, can't set a closed resource to idle"
      | `Idle -> failwith "Impossible, already marked as idle"
      | `In_use_until done_ ->
        assert (Ivar.is_empty done_);
        if t.remaining_uses <= 0
        then don't_wait_for (close t)
        else (
          set_state t `Idle;
          Ivar.fill_exn done_ ();
          Clock_ns.after t.config.idle_cleanup_after
          >>> fun () ->
          match t.state with
          | `Closing | `In_use_until _ -> ()
          | `Idle ->
            let idle_time = Time_ns.diff (Time_ns.now ()) t.in_state_since in
            if Time_ns.Span.( >= ) idle_time t.config.idle_cleanup_after
            then don't_wait_for (close t))
    ;;

    let unsafe_immediate t ~f =
      match t.state with
      | `Closing -> failwith "Can't [unsafe_immediate] a closed resource"
      | `Idle -> failwith "Can't [unsafe_immediate] an idle resource"
      | `In_use_until done_ ->
        assert (Ivar.is_empty done_);
        assert (t.remaining_uses > 0);
        t.remaining_uses <- t.remaining_uses - 1;
        (* deliberately not filling [done_] here. It is filled in [set_idle] or [close]. *)
        let result =
          Monitor.try_with ~run:`Schedule ~rest:`Log (fun () ->
            f (Set_once.get_exn t.resource))
        in
        upon result (function
          | Ok _ -> set_idle t
          | Error _ -> don't_wait_for (Deferred.ignore_m (close t)));
        result
    ;;

    let immediate t ~f =
      match t.state with
      | `Closing -> `Resource_closed
      | `In_use_until done_ -> `Resource_unavailable_until (Ivar.read done_)
      | `Idle ->
        (* It is possible that [R.close] was called but [R.close_finished] is not
           determined yet. Use [R.is_closed] to prevent this race. *)
        if R.has_close_started (Set_once.get_exn t.resource)
        then `Resource_closed
        else (
          set_state t (`In_use_until (Ivar.create ()));
          `Result (unsafe_immediate t ~f))
    ;;

    let create ?open_timeout ?on_state_update config key args ~with_ ~log_error =
      let t =
        { id = Id.create ()
        ; key
        ; args
        ; resource = Set_once.create ()
        ; state = `In_use_until (Ivar.create ())
        ; in_state_since = Time_ns.now ()
        ; config
        ; remaining_uses = config.Config.max_resource_reuse
        ; close_finished = Ivar.create ()
        ; on_state_update
        ; log_error
        }
      in
      let maybe_catch_unhandled_exns_and_close ~f =
        (let resource = ref None in
         let close_on_exn exn =
           !Monitor.Expert.try_with_log_exn
             (Error.create_s
                [%message
                  "Resource raised exn after creation" (key : R.Key.t) ~_:(exn : Exn.t)]
              |> Error.to_exn);
           Option.iter !resource ~f:(fun resource ->
             don't_wait_for
               (Deferred.ignore_m
                  (Monitor.try_with ~run:`Schedule ~rest:`Log (fun () -> R.close resource))))
         in
         let name =
           if am_running_test then None else Some (Source_code_position.to_string [%here])
         in
         Monitor.try_with
           ~run:`Schedule
           ?name
           ~rest:
             (if config.close_resource_on_unhandled_exn then `Call close_on_exn else `Log)
           (fun () ->
             let%map.Deferred.Or_error res = f () in
             resource := Some res;
             res))
        >>| Result.map_error ~f:Error.of_exn
        |> Deferred.map ~f:Or_error.join
      in
      let res =
        let error_or_close_while_opening result =
          (* Ensure [close_finished] gets filled *)
          don't_wait_for (close t);
          return result
        in
        match%bind
          maybe_catch_unhandled_exns_and_close ~f:(fun () ->
            match open_timeout with
            | None -> R.open_ key args
            | Some timeout ->
              let resource_ivar = Ivar.create () in
              (match%map
                 Clock_ns.with_timeout
                   timeout
                   (let%map r = R.open_ key args in
                    Ivar.fill_exn resource_ivar r;
                    r)
               with
               | `Result r -> r
               | `Timeout ->
                 (* In case we timeout, make sure we cleanup after ourselves *)
                 (Ivar.read resource_ivar
                  >>> function
                  | Error _ -> ()
                  | Ok r -> don't_wait_for (R.close r));
                 Or_error.error_string "Exceeded open timeout while creating resource"))
        with
        | Ok res ->
          (* A call to [close_and_flush] might have occurred *)
          if t.remaining_uses > 0
          then (
            don't_wait_for
              (let%bind () = R.close_finished res in
               close_when_idle t);
            Set_once.set_exn t.resource res;
            let%map r = unsafe_immediate t ~f:with_ in
            `Result (key, r))
          else error_or_close_while_opening `Cache_is_closed
        | Error err -> error_or_close_while_opening (`Error_opening_resource (key, err))
      in
      t, res
    ;;
  end

  module Idle_resource_tracker : sig
    type t

    val create : unit -> t
    val close_least_recently_used : t -> unit

    val on_resource_state_update
      :  t
      -> (Resource.t -> [ `Idle | `In_use_until of unit Ivar.t | `Closing ] -> unit)
           Staged.t
  end = struct
    module Lru = Hash_queue.Make (Resource.Id)

    type t = Resource.t Lru.t

    let create () = Lru.create ()

    let enqueue t resource =
      let id = Resource.id resource in
      match Lru.enqueue_back t id resource with
      | `Ok -> ()
      | `Key_already_present ->
        (* This shouldn't happen, but this is the logical thing to do. *)
        ignore (Lru.lookup_and_move_to_back_exn t id : Resource.t)
    ;;

    let remove t resource =
      match Lru.remove t (Resource.id resource) with
      | `Ok -> ()
      | `No_such_key ->
        (* This can occur because [close_least_recently_used] removes from the queue and a
           subsequent state update comes later. *)
        ()
    ;;

    let close_least_recently_used t =
      (* Explicitly dequeue so another call to [close_least_recently_used] before the
         resource's [on_state_update] fires will choose a different resource. *)
      match Lru.dequeue_front t with
      | Some resource ->
        don't_wait_for (Resource.close_when_idle resource);
        ()
      | None -> ()
    ;;

    let on_resource_state_update t =
      stage (fun resource -> function
        | `Idle -> enqueue t resource
        | `In_use_until _ | `Closing -> remove t resource)
    ;;
  end

  (* Limit the number of concurrent [Resource.t]s globally *)
  module Global_resource_limiter : sig
    type t

    val create : Config.t -> t

    (* create a single resource, and block a slot until the resource has been cleaned up *)

    val create_resource
      :  ?open_timeout:Time_ns.Span.t
      -> t
      -> R.Key.t
      -> R.Common_args.t
      -> with_:(R.t -> 'a Deferred.t)
      -> log_error:(string -> unit)
      -> [ `Result of
           Resource.t
           * [> `Result of R.Key.t * ('a, exn) Result.t | Delayed_failures.t ] Deferred.t
         | `Cache_is_closed
         | `No_resource_available_until of unit Deferred.t
         ]

    val maybe_close_least_recently_used : t -> unit
    val close_and_flush : t -> unit Deferred.t
  end = struct
    type t =
      { config : Config.t
      ; idle_resource_tracker : Idle_resource_tracker.t option
      ; throttle : unit Throttle.t
      }

    let create config =
      { config
      ; idle_resource_tracker =
          (if config.close_idle_resources_when_at_limit
           then Some (Idle_resource_tracker.create ())
           else None)
      ; throttle =
          Throttle.create
            ~continue_on_error:true
            ~max_concurrent_jobs:config.max_resources
      }
    ;;

    let capacity_available_now t =
      Throttle.num_jobs_running t.throttle < Throttle.max_concurrent_jobs t.throttle
    ;;

    let create_resource ?open_timeout t key args ~with_ ~log_error =
      if Throttle.is_dead t.throttle
      then `Cache_is_closed
      else if capacity_available_now t
      then (
        assert (Throttle.num_jobs_waiting_to_start t.throttle = 0);
        let on_state_update =
          Option.map t.idle_resource_tracker ~f:(fun tracker ->
            unstage (Idle_resource_tracker.on_resource_state_update tracker))
        in
        let r, v =
          Resource.create
            ?open_timeout
            ?on_state_update
            t.config
            key
            args
            ~with_
            ~log_error
        in
        don't_wait_for (Throttle.enqueue t.throttle (fun () -> Resource.close_finished r));
        `Result (r, v))
      else
        `No_resource_available_until
          (Deferred.any
             [ Throttle.capacity_available t.throttle; Throttle.cleaned t.throttle ])
    ;;

    let maybe_close_least_recently_used t =
      match t.idle_resource_tracker with
      | None -> ()
      | Some tracker ->
        if not (capacity_available_now t)
        then Idle_resource_tracker.close_least_recently_used tracker
    ;;

    let close_and_flush t =
      Throttle.kill t.throttle;
      Throttle.cleaned t.throttle
    ;;
  end

  (* Limit the number of concurrent [Resource.t]s locally *)
  module Resource_list : sig
    type t

    val create
      :  Config.t
      -> Global_resource_limiter.t
      -> R.Key.t
      -> R.Common_args.t
      -> log_error:(string -> unit)
      -> t

    val status : t -> Status.Resource_list.t

    (* [is_empty] is true iff there are no currently connected/connecting resources. *)

    val is_empty : t -> bool

    (* [close_and_flush'] will mark this resource list for removal and start tearing down
       all its resources. *)

    val close_and_flush' : t -> unit

    (* [close_finished] becomes determined after [close_and_flush'] was called and all
       resources have been closed. *)

    val close_finished : t -> unit Deferred.t

    (** [close_all_resources] will teardown all currently open resources. Any further
        calls to [find_available_resource], [create_resource] (and any not yet completed
        [enqueue] calls) will result in new resources getting created. *)
    val close_all_resources : t -> unit Deferred.t

    (* [find_available_resource] and [create_resource] can be used to bypass [enqueue] in
       the case where there is an idle resource or an available slot. *)

    val find_available_resource
      :  t
      -> f:(R.t -> 'a Deferred.t)
      -> [ `Immediate of ('a, exn) Deferred.Result.t
         | `None_until of unit Deferred.t
         | `Cache_is_closed
         ]

    val create_resource
      :  ?open_timeout:Time_ns.Span.t
      -> t
      -> f:(R.t -> 'a Deferred.t)
      -> [> `Result of R.Key.t * ('a, exn) Result.t | Delayed_failures.t ] Deferred.t
           option

    val any_resource_closed : t -> unit Deferred.t

    (** [enqueue] the job to wait on an entry in this resource list to become available.
        If this [resource_list] is closed the job will be marked as `Cache_is_closed. *)
    val enqueue : t -> 'a Job.t -> unit

    val num_open : t -> int
  end = struct
    type job = T : 'a Job.t -> job

    type t =
      { config : Config.t
      ; key : R.Key.t
      ; args : R.Common_args.t
      ; global_resource_limiter : Global_resource_limiter.t
      ; resources : Resource.t Resource.Id.Table.t
      ; waiting_jobs : job Doubly_linked.t
      ; trigger_queue_manager : unit Mvar.Read_write.t
      ; any_resource_closed_bvar : (unit, read_write) Bvar.t
      ; mutable close_started : bool
      ; close_finished : unit Ivar.t
      ; log_error : string -> unit
      }

    let num_open t = Hashtbl.length t.resources
    let any_resource_closed t = Bvar.wait t.any_resource_closed_bvar

    let status t =
      let max_time_on_queue =
        Doubly_linked.first t.waiting_jobs
        |> Option.map ~f:(fun (T job) ->
          Time_ns.diff (Time_ns.now ()) (Job.created_at job))
      in
      { Status.Resource_list.key = t.key
      ; resources = Hashtbl.data t.resources |> List.map ~f:Resource.status
      ; queue_length = Doubly_linked.length t.waiting_jobs
      ; max_time_on_queue
      }
    ;;

    let find_available_resource t ~f =
      if t.close_started
      then `Cache_is_closed
      else
        With_return.with_return (fun { return } ->
          let until =
            Hashtbl.fold t.resources ~init:[] ~f:(fun ~key:_ ~data:r until ->
              match Resource.immediate r ~f with
              | `Result r -> return (`Immediate r)
              | `Resource_unavailable_until u -> u :: until
              | `Resource_closed -> until)
          in
          `None_until (Deferred.any until))
    ;;

    let create_resource ?open_timeout t ~f =
      if t.close_started
      then Some (return `Cache_is_closed)
      else if Hashtbl.length t.resources >= t.config.max_resources_per_id
      then None
      else (
        match
          Global_resource_limiter.create_resource
            ?open_timeout
            t.global_resource_limiter
            t.key
            t.args
            ~with_:f
            ~log_error:t.log_error
        with
        | `Cache_is_closed -> None
        | `No_resource_available_until u ->
          (* Trigger when there is global capacity available *)
          upon u (Mvar.set t.trigger_queue_manager);
          None
        | `Result (resource, response) ->
          Hashtbl.add_exn t.resources ~key:(Resource.id resource) ~data:resource;
          (Resource.close_finished resource
           >>> fun () ->
           Hashtbl.remove t.resources (Resource.id resource);
           (* Trigger that capacity is now available *)
           Mvar.set t.trigger_queue_manager ();
           Bvar.broadcast t.any_resource_closed_bvar ();
           if t.close_started && Hashtbl.is_empty t.resources
           then Ivar.fill_exn t.close_finished ());
          (* Trigger when this resource is now available. This is needed because
             [create_resource] is called from outside this module *)
          upon response (fun _ -> Mvar.set t.trigger_queue_manager ());
          Some response)
    ;;

    let allocate_resources t =
      let rec loop () =
        match Doubly_linked.first t.waiting_jobs with
        | None -> ()
        | Some (T job) ->
          (* Skip if this job has a result already *)
          if Job.has_result job
          then (
            let (_ : job option) = Doubly_linked.remove_first t.waiting_jobs in
            loop ())
          else (
            match find_available_resource t ~f:(Job.f job) with
            | `Immediate result ->
              Job.mark_result_from_available_resource job t.key ~result;
              let (_ : job option) = Doubly_linked.remove_first t.waiting_jobs in
              loop ()
            | `None_until until ->
              (* Trigger when a resource is available *)
              upon until (Mvar.set t.trigger_queue_manager);
              (match
                 create_resource ?open_timeout:(Job.open_timeout job) t ~f:(Job.f job)
               with
               | Some result ->
                 Job.mark_result_from_resource_creation job ~result;
                 let (_ : job option) = Doubly_linked.remove_first t.waiting_jobs in
                 loop ()
               | None -> ())
            | `Cache_is_closed ->
              raise_s
                [%message
                  "BUG: We clear the waiting_jobs when setting close_started and do not \
                   add once close_started is set."])
      in
      loop ()
    ;;

    let start_background_resource_allocator t =
      Deferred.repeat_until_finished () (fun () ->
        let%map () = Mvar.take t.trigger_queue_manager in
        if t.close_started
        then `Finished ()
        else (
          allocate_resources t;
          `Repeat ()))
    ;;

    let create config global_resource_limiter key args ~log_error =
      let t =
        { config
        ; key
        ; args
        ; global_resource_limiter
        ; resources = Resource.Id.Table.create ()
        ; waiting_jobs = Doubly_linked.create ()
        ; trigger_queue_manager = Mvar.create ()
        ; any_resource_closed_bvar = Bvar.create ()
        ; close_started = false
        ; close_finished = Ivar.create ()
        ; log_error
        }
      in
      don't_wait_for (start_background_resource_allocator t);
      t
    ;;

    let enqueue t job =
      if t.close_started
      then Job.mark_cache_closed job
      else (
        let inserted_job = Doubly_linked.insert_last t.waiting_jobs (T job) in
        (* Trigger that a new job is on the queue *)
        Mvar.set t.trigger_queue_manager ();
        upon (Job.result job) (fun _ ->
          if Doubly_linked.mem_elt t.waiting_jobs inserted_job
          then Doubly_linked.remove t.waiting_jobs inserted_job;
          (* Trigger that a resource is now available *)
          Mvar.set t.trigger_queue_manager ()))
    ;;

    let is_empty t = Hashtbl.is_empty t.resources && Doubly_linked.is_empty t.waiting_jobs
    let close_finished t = Ivar.read t.close_finished

    let close_all_resources t =
      let%map () =
        Hashtbl.data t.resources
        |> List.map ~f:(fun r -> Resource.close_when_idle r)
        |> Deferred.all_unit
      in
      Ivar.fill_if_empty t.close_finished ()
    ;;

    let close_and_flush' t =
      if not t.close_started
      then (
        t.close_started <- true;
        Doubly_linked.iter t.waiting_jobs ~f:(fun (T job) -> Job.mark_cache_closed job);
        Doubly_linked.clear t.waiting_jobs;
        don't_wait_for (close_all_resources t))
      else ()
    ;;
  end

  type t =
    { config : Config.t
    ; global_resource_limiter : Global_resource_limiter.t
    ; cache : Resource_list.t R.Key.Table.t
    ; args : R.Common_args.t
    ; mutable num_jobs_in_cache : int
    ; mutable close_started : bool
    ; close_finished : unit Ivar.t
    ; log_error : string -> unit
    }

  let num_open t key =
    match Hashtbl.find_or_null t.cache key with
    | Null -> 0
    | This resource_list -> Resource_list.num_open resource_list
  ;;

  let status t =
    let resource_lists = List.map (Hashtbl.data t.cache) ~f:Resource_list.status in
    { Status.resource_lists; num_jobs_in_cache = t.num_jobs_in_cache }
  ;;

  let get_resource_list t key =
    Hashtbl.find_or_add t.cache key ~default:(fun () ->
      Resource_list.create
        t.config
        t.global_resource_limiter
        key
        t.args
        ~log_error:t.log_error)
  ;;

  let find_any_available_resource t keys ~f =
    List.find_map keys ~f:(fun key ->
      let res_list = get_resource_list t key in
      match Resource_list.find_available_resource res_list ~f with
      | `Immediate res -> Some (key, res)
      | `None_until _ | `Cache_is_closed -> None)
  ;;

  let create_any_resource ?open_timeout t keys ~f =
    List.find_map keys ~f:(fun key ->
      let res_list = get_resource_list t key in
      Resource_list.create_resource ?open_timeout res_list ~f)
  ;;

  let enqueue_all ?open_timeout t ~give_up keys ~f =
    let job = Job.create ?open_timeout ~give_up ~f in
    List.iter keys ~f:(fun key ->
      let res_list = get_resource_list t key in
      Resource_list.enqueue res_list job);
    Job.result job
  ;;

  let with_any' ?open_timeout ?(give_up = Deferred.never ()) t keys ~f =
    let f resource = f (R.underlying resource) in
    t.num_jobs_in_cache <- t.num_jobs_in_cache + 1;
    let raise_and_cleanup exn =
      t.num_jobs_in_cache <- t.num_jobs_in_cache - 1;
      raise exn
    in
    let result =
      if t.close_started
      then return `Cache_is_closed
      else (
        match find_any_available_resource t keys ~f with
        | Some (args, res) ->
          (match%map res with
           | Ok res -> `Ok (args, res)
           | Error exn -> raise_and_cleanup exn)
        | None ->
          (match create_any_resource ?open_timeout t keys ~f with
           | Some res ->
             (match%map res with
              | `Result (_key, Error exn) -> raise_and_cleanup exn
              | `Result (key, Ok res) -> `Ok (key, res)
              | (`Error_opening_resource _ | `Cache_is_closed) as res -> res)
           | None ->
             Global_resource_limiter.maybe_close_least_recently_used
               t.global_resource_limiter;
             if Deferred.is_determined give_up
             then return `Gave_up_waiting_for_resource
             else (
               match%bind enqueue_all ?open_timeout ~give_up t keys ~f with
               | `Result (_key, Error exn) -> raise_and_cleanup exn
               | `Result (key, Ok res) -> return (`Ok (key, res))
               | ( `Cache_is_closed
                 | `Error_opening_resource _
                 | `Gave_up_waiting_for_resource ) as res -> return res)))
    in
    upon result (fun _ -> t.num_jobs_in_cache <- t.num_jobs_in_cache - 1);
    result
  ;;

  let with_any ?open_timeout ?give_up t keys ~f =
    match%map with_any' ?open_timeout ?give_up t keys ~f with
    | `Ok args_and_res -> Ok args_and_res
    | `Error_opening_resource (key, err) ->
      let tag = sprintf !"Error creating required resource: %{sexp:R.Key.t}" key in
      Error (Error.tag ~tag err)
    | `Cache_is_closed -> Or_error.error_string "Cache is closed"
    | `Gave_up_waiting_for_resource ->
      Or_error.error_string "Gave up waiting for resource"
  ;;

  let with_ ?open_timeout ?give_up t key ~f =
    match%map with_any ?open_timeout ?give_up t [ key ] ~f with
    | Ok (_args, res) -> Ok res
    | Error e -> Error e
  ;;

  let with_' ?open_timeout ?give_up t key ~f =
    match%map with_any' ?open_timeout ?give_up t [ key ] ~f with
    | `Ok (_args, res) -> `Ok res
    | `Error_opening_resource (_args, err) -> `Error_opening_resource err
    | `Cache_is_closed -> `Cache_is_closed
    | `Gave_up_waiting_for_resource -> `Gave_up_waiting_for_resource
  ;;

  let with_any_loop ?open_timeout ?give_up t keys ~f =
    let rec loop ~failed = function
      | [] -> return (`Error_opening_all_resources (List.rev failed))
      | keys ->
        (match%bind with_any' ?open_timeout ?give_up t keys ~f with
         | (`Ok _ | `Gave_up_waiting_for_resource | `Cache_is_closed) as res -> return res
         | `Error_opening_resource (failed_key, e) ->
           let remaining =
             List.filter keys ~f:(fun key -> not (R.Key.equal key failed_key))
           in
           loop ~failed:((failed_key, e) :: failed) remaining)
    in
    loop ~failed:[] keys
  ;;

  let init ~config ~log_error args =
    let t =
      { config
      ; global_resource_limiter = Global_resource_limiter.create config
      ; cache = R.Key.Table.create ()
      ; args
      ; num_jobs_in_cache = 0
      ; close_started = false
      ; close_finished = Ivar.create ()
      ; log_error
      }
    in
    Clock_ns.every ~stop:(Ivar.read t.close_finished) config.idle_cleanup_after (fun () ->
      Hashtbl.filter_inplace t.cache ~f:(fun d ->
        if Resource_list.is_empty d
        then (
          Resource_list.close_and_flush' d;
          false)
        else true));
    t
  ;;

  let close_and_flush t =
    if not t.close_started
    then (
      t.close_started <- true;
      let%map () =
        Deferred.all_unit
          (Global_resource_limiter.close_and_flush t.global_resource_limiter
           :: List.map (Hashtbl.data t.cache) ~f:(fun r ->
             Resource_list.close_and_flush' r;
             Resource_list.close_finished r))
      in
      Ivar.fill_exn t.close_finished ())
    else Ivar.read t.close_finished
  ;;

  let close_all_resources_for_key t key =
    match Hashtbl.find_or_null t.cache key with
    | Null ->
      (* No resources for this key - nothing to do *)
      return ()
    | This resource_list ->
      (* Close all currently open resources. We do not remove this from the table here so
         we continue to account for the open resources as they get cleaned up and so that
         any queued jobs will continue to work as intended. This will get cleaned up once
         all connections have been closed. *)
      Resource_list.close_all_resources resource_list
  ;;

  let config t = t.config
  let close_started t = t.close_started
  let close_finished t = Ivar.read t.close_finished

  let validate_cache_warming_args t ~caller ~num_resources_to_keep_open_per_key keys =
    if num_resources_to_keep_open_per_key <= 0
       || num_resources_to_keep_open_per_key > t.config.max_resources_per_id
       || num_resources_to_keep_open_per_key * List.length keys > t.config.max_resources
    then
      raise_s
        [%message
          [%string
            "Invalid value for [num_resources_to_keep_open_per_key] in call to \
             [Resource_cache.%{caller}]: must be greater than 0, less than or equal to \
             [config.max_resources_per_id], and [num_resources_to_keep_open_per_key * \
             List.length keys] must be less than or equal to [config.max_resources]"]
            (num_resources_to_keep_open_per_key : int)
            (List.length keys : int)
            (t.config.max_resources_per_id : int)
            (t.config.max_resources : int)]
  ;;

  let warm_cache_single_key_unchecked
    t
    ~on_error_opening_resource
    ~num_resources_to_keep_open_per_key
    key
    =
    if close_started t
    then return ()
    else (
      let resource_list = get_resource_list t key in
      let num_open = Resource_list.num_open resource_list in
      let num_to_open = num_resources_to_keep_open_per_key - num_open in
      if num_to_open <= 0
      then return ()
      else
        Deferred.List.all_unit
          (List.init num_to_open ~f:(fun (_ : int) ->
             match
               Resource_list.create_resource resource_list ~f:(fun (_ : R.t) -> return ())
             with
             | None ->
               (* We hit the global capacity limit - it can't be the per-key limit or the
                  whole cache being closed, because we checked above and haven't yielded
                  yet.

                  Either way, there's nothing we can do here. *)
               return ()
             | Some res ->
               (match%map res with
                | `Error_opening_resource (key, error) ->
                  on_error_opening_resource ~key ~error
                | `Result ((_ : R.Key.t), Ok ()) | `Cache_is_closed -> ()
                | `Result ((_ : R.Key.t), Error exn) ->
                  (* This is not possible because the [f] we supply to
                     [Resource_list.create_resource] just returns unit. *)
                  raise_s
                    [%message
                      "BUG: got an exception when running [f] on resource" (exn : Exn.t)]))))
  ;;

  let warm_cache t ~on_error_opening_resource ~num_resources_to_keep_open_per_key keys =
    validate_cache_warming_args
      t
      ~caller:"warm_cache"
      ~num_resources_to_keep_open_per_key
      keys;
    if close_started t
    then return ()
    else
      Deferred.List.iter keys ~how:`Parallel ~f:(fun key ->
        warm_cache_single_key_unchecked
          t
          ~on_error_opening_resource
          ~num_resources_to_keep_open_per_key
          key)
  ;;

  let keep_cache_warm
    t
    ~on_error_opening_resource
    ~num_resources_to_keep_open_per_key
    keys
    =
    validate_cache_warming_args
      t
      ~caller:"keep_cache_warm"
      ~num_resources_to_keep_open_per_key
      keys;
    List.iter keys ~f:(fun key ->
      let wait_until_timeout_or_resource_closed () =
        Clock_ns.with_timeout
          (* if for some reason we miss a bvar notification have a timeout so we'll always
             retry anyway *)
          (Time_ns.Span.of_sec 60.)
          (Deferred.any_unit
             [ close_finished t
               (* wakeup during shutdown, though [any_resource_closed] should trigger
                  also. *)
             ; Resource_list.any_resource_closed (get_resource_list t key)
             ])
        >>| (ignore : unit Clock_ns.Or_timeout.t -> unit)
      in
      let num_to_open () =
        Int.max (num_resources_to_keep_open_per_key - num_open t key) 0
      in
      Deferred.repeat_until_finished () (fun () ->
        if close_started t
        then return (`Finished ())
        else if num_to_open () <= 0
        then (
          let%map () = wait_until_timeout_or_resource_closed () in
          `Repeat ())
        else (
          let%map () =
            warm_cache_single_key_unchecked
              t
              ~on_error_opening_resource
              ~num_resources_to_keep_open_per_key
              key
          in
          `Repeat ()))
      |> don't_wait_for)
  ;;
end

module Make (R : Resource.S) () = struct
  include
    Make_wrapped
      (struct
        include R

        type resource = t

        let underlying t = t
      end)
      ()
end

module Make_simple (R : Resource.Simple) () = struct
  include
    Make_wrapped
      (struct
        include Resource.Make_simple (R)
      end)
      ()
end
