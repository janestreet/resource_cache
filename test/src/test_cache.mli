open! Core

module Resource : sig
  include T

  module Common_args : sig
    type t

    val default : t
  end
end

module Test_cache : sig
  include
    Resource_cache.S
    with type key := int
     and type common_args := Resource.Common_args.t
     and type resource := Resource.t

  val init : config:Resource_cache.Config.t -> Resource.Common_args.t -> t
end
