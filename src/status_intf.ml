open! Core_kernel
open! Async_kernel
open! Import

module type S = sig
  module Key : sig
    type t [@@deriving sexp_of]

    include Comparable.S_plain with type t := t
    include Hashable.S_plain with type t := t
  end

  module Resource : sig
    type state =
      [ `Busy
      | `Idle
      | `Closing ]
    [@@deriving sexp_of, compare]

    type t =
      { state : state
      ; since : Time_ns.Span.t
      }
    [@@deriving fields, sexp_of, compare]
  end

  module Resource_list : sig
    type t =
      { key : Key.t
      ; resources : Resource.t list
      ; queue_length : int
      ; max_time_on_queue : Time_ns.Span.t option
      }
    [@@deriving fields, sexp_of, compare]
  end

  type t =
    { resource_lists : Resource_list.t list
    ; num_jobs_in_cache : int
    }
  [@@deriving fields, sexp_of, compare]
end

module type Status = sig
  module type S = S
end
