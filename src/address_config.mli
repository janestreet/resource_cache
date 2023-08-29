open! Core
open! Import

type t =
  { max_open_connections : int
  ; cleanup_idle_connection_after : Time_ns.Span.t
  ; max_connections_per_address : int
  ; max_connection_reuse : int
  ; close_idle_connections_when_at_limit : bool
  ; close_connection_on_unhandled_exn : bool
  }
[@@deriving compare, sexp_of]

val create
  :  max_open_connections:int
  -> cleanup_idle_connection_after:Time_ns.Span.t
  -> max_connections_per_address:int
  -> max_connection_reuse:int
  -> close_idle_connections_when_at_limit:bool
  -> close_connection_on_unhandled_exn:bool
  -> t

val default : t
val to_cache_config : t -> Config.t
val of_cache_config : Config.t -> t

module Stable : sig
  module V3 : sig
    type nonrec t = t [@@deriving bin_io, sexp]
  end

  module V2 : sig
    type t =
      { max_open_connections : int
      ; cleanup_idle_connection_after : Time_ns.Span.t
      ; max_connections_per_address : int
      ; max_connection_reuse : int
      ; close_idle_connections_when_at_limit : bool
      }
    [@@deriving bin_io, sexp]

    val of_v3 : V3.t -> t
    val to_v3 : t -> V3.t
  end

  module V1 : sig
    type t =
      { max_open_connections : int
      ; cleanup_idle_connection_after : Time_ns.Span.t
      ; max_connections_per_address : int
      ; max_connection_reuse : int
      }
    [@@deriving bin_io, sexp]

    val of_v2 : V2.t -> t
    val to_v2 : t -> V2.t
  end
end
