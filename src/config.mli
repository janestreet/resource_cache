open! Core
open! Async_kernel
open! Import

type t =
  { max_resources : int
  ; idle_cleanup_after : Time_ns.Span.t
  ; max_resources_per_id : int
  ; max_resource_reuse : int
  ; close_idle_resources_when_at_limit : bool
  (** The cache will close the least recently used idle resource when there is pressure at
      the [max_resources] limit. *)
  ; close_resource_on_unhandled_exn : bool
  (** if [close_resource_on_unhandled_exn] is set to true, when the resource raises an exn
      after creation, it will be [close]d. *)
  }
[@@deriving compare, sexp_of]

val create
  :  max_resources:int
  -> idle_cleanup_after:Time_ns.Span.t
  -> max_resources_per_id:int
  -> max_resource_reuse:int
  -> close_idle_resources_when_at_limit:bool
  -> close_resource_on_unhandled_exn:bool
  -> t

module Stable : sig
  module V3 : sig
    type nonrec t = t [@@deriving bin_io, sexp]
  end

  module V2 : sig
    type nonrec t =
      { max_resources : int
      ; idle_cleanup_after : Time_ns.Span.t
      ; max_resources_per_id : int
      ; max_resource_reuse : int
      ; close_idle_resources_when_at_limit : bool
      }
    [@@deriving bin_io, sexp]

    val to_v3 : t -> V3.t
  end

  module V1 : sig
    type t =
      { max_resources : int
      ; idle_cleanup_after : Time_ns.Span.t
      ; max_resources_per_id : int
      ; max_resource_reuse : int
      }
    [@@deriving bin_io, sexp]

    val to_v2 : t -> V2.t
  end
end
