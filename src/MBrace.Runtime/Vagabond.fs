namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.Reflection
open System.IO

open Nessos.Vagabond

open MBrace.Core.Internals

/// Vagabond state container
type VagabondRegistry private () =

    static let lockObj = obj()
    static let mutable instance : VagabondManager option = None

    /// Gets the registered vagabond instance.
    static member Instance =
        match instance with
        | None -> invalidOp "No instance of vagabond has been registered."
        | Some instance -> instance

    /// Gets whether a Vagabond instance has been registered.
    static member IsRegistered = Option.isSome instance

    /// Gets the current configuration of the Vagabond registry.
    static member Configuration = VagabondRegistry.Instance.Configuration

    /// <summary>
    ///     Initializes the registry using provided factory.
    /// </summary>
    /// <param name="factory">Vagabond instance factory. Defaults to default factory.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize(?factory : unit -> VagabondManager, ?throwOnError : bool) =
        let factory = defaultArg factory (fun () -> Vagabond.Initialize())
        lock lockObj (fun () ->
            match instance with
            | None -> instance <- Some <| factory ()
            | Some _ when defaultArg throwOnError true -> invalidOp "An instance of Vagabond has already been registered."
            | Some _ -> ())

    /// <summary>
    ///     Initializes the registry using provided configuration object.
    /// </summary>
    /// <param name="config">Vagabond configuration object.</param>
    static member Initialize(config : VagabondConfiguration, ?throwOnError : bool) =
        VagabondRegistry.Initialize((fun () -> Vagabond.Initialize(config)), ?throwOnError = throwOnError)