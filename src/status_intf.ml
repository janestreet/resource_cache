open! Core
open! Async

module type S = sig
  module Key : Identifiable.S

  module Resource : sig
    type state = [`Busy | `Idle | `Closing] [@@deriving sexp, bin_io, compare]

    type t =
      { state : state
      ; since : Time.Span.t
      }
    [@@deriving fields, sexp, bin_io, compare]
  end

  module Resource_list : sig
    type t =
      { key : Key.t
      ; resources : Resource.t list
      ; queue_length : int
      ; max_time_on_queue : Time.Span.t option
      }
    [@@deriving fields, sexp, bin_io, compare]
  end

  type t =
    { resource_lists : Resource_list.t list
    ; num_jobs_in_cache : int
    }
  [@@deriving fields, sexp, bin_io, compare]
end

module type Status = sig
  module type S = S
end
