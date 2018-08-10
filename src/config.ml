open! Core
open! Async

type t =
  { max_resources : int
  ; idle_cleanup_after : Time.Span.t
  ; max_resources_per_id : int
  ; max_resource_reuse : int
  }
[@@deriving fields, sexp, bin_io, compare]

let create = Fields.create
