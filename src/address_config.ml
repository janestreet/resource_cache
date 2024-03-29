module Stable = struct
  open! Core.Core_stable

  module V3 = struct
    type t =
      { max_open_connections : int
      ; cleanup_idle_connection_after : Time_ns.Span.V2.t
      ; max_connections_per_address : int
      ; max_connection_reuse : int
      ; close_idle_connections_when_at_limit : bool
      ; close_connection_on_unhandled_exn : bool
      }
    [@@deriving bin_io, sexp]

    let%expect_test _ =
      print_endline [%bin_digest: t];
      [%expect {| 03db3ce55a1aa6bbda6faa70d3b3e86b |}]
    ;;
  end

  module V2 = struct
    type t =
      { max_open_connections : int
      ; cleanup_idle_connection_after : Time_ns.Span.V2.t
      ; max_connections_per_address : int
      ; max_connection_reuse : int
      ; close_idle_connections_when_at_limit : bool
      }
    [@@deriving
      bin_io, sexp, stable_record ~version:V3.t ~add:[ close_connection_on_unhandled_exn ]]

    let of_v3 = of_V3_t
    let to_v3 = to_V3_t ~close_connection_on_unhandled_exn:false

    let%expect_test _ =
      print_endline [%bin_digest: t];
      [%expect {| c8122cfd57d06e0d9201489d1d070af5 |}]
    ;;
  end

  module V1 = struct
    type t =
      { max_open_connections : int
      ; cleanup_idle_connection_after : Time_ns.Span.V2.t
      ; max_connections_per_address : int
      ; max_connection_reuse : int
      }
    [@@deriving
      bin_io
      , sexp
      , stable_record ~version:V2.t ~add:[ close_idle_connections_when_at_limit ]]

    let of_v2 = of_V2_t
    let to_v2 = to_V2_t ~close_idle_connections_when_at_limit:false

    let%expect_test _ =
      print_endline [%bin_digest: t];
      [%expect {| f6909d04e51fd189259fe7dbe513e5a5 |}]
    ;;
  end
end

open! Core
open! Async_kernel
open! Import

type t = Stable.V3.t =
  { max_open_connections : int
  ; cleanup_idle_connection_after : Time_ns.Span.t
  ; max_connections_per_address : int
  ; max_connection_reuse : int
  ; close_idle_connections_when_at_limit : bool
  ; close_connection_on_unhandled_exn : bool
  }
[@@deriving compare, fields ~iterators:create, sexp_of]

let create = Fields.create

let default =
  { max_open_connections = 500
  ; cleanup_idle_connection_after = Time_ns.Span.of_sec 5.
  ; max_connections_per_address = 10
  ; max_connection_reuse = 10
  ; close_idle_connections_when_at_limit = false
  ; close_connection_on_unhandled_exn = true
  }
;;

let to_cache_config t =
  Config.create
    ~max_resources:t.max_open_connections
    ~idle_cleanup_after:t.cleanup_idle_connection_after
    ~max_resources_per_id:t.max_connections_per_address
    ~max_resource_reuse:t.max_connection_reuse
    ~close_idle_resources_when_at_limit:t.close_idle_connections_when_at_limit
    ~close_resource_on_unhandled_exn:t.close_connection_on_unhandled_exn
;;

let of_cache_config (cache_config : Config.t) =
  create
    ~max_open_connections:cache_config.max_resources
    ~cleanup_idle_connection_after:cache_config.idle_cleanup_after
    ~max_connections_per_address:cache_config.max_resources_per_id
    ~max_connection_reuse:cache_config.max_resource_reuse
    ~close_idle_connections_when_at_limit:cache_config.close_idle_resources_when_at_limit
    ~close_connection_on_unhandled_exn:cache_config.close_resource_on_unhandled_exn
;;
