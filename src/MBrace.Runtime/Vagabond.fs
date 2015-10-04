namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.Reflection
open System.IO

open Nessos.Vagabond

open MBrace.Core.Internals

/// Global Vagabond instance container
type VagabondRegistry private () =

    static let lockObj = obj()
    static let mutable instance : VagabondManager option = None

    static let registerVagabond (throwOnError : bool) (factory : unit -> VagabondManager) =
        lock lockObj (fun () ->
            match instance with
            | None -> instance <- Some <| factory ()
            | Some _ when throwOnError -> invalidOp "An instance of Vagabond has already been registered."
            | Some _ -> ())

    /// Gets the registered vagabond instance.
    static member Instance =
        match instance with
        | None -> invalidOp "No instance of vagabond has been registered."
        | Some instance -> instance

    /// Gets whether a Vagabond instance has been registered.
    static member IsRegistered = Option.isSome instance

    /// <summary>
    ///     Initializes the Vagabond registry with provided parameters.
    /// </summary>
    /// <param name="workingDirectory">Working directory used by Vagabond. Defaults to self-assigned directory.</param>
    /// <param name="isClientSession">Indicates that Vagabond instance is for usage by MBrace client. Defaults to false.</param>
    static member Initialize(?workingDirectory : string, ?isClientSession : bool) =
        let isClientSession = defaultArg isClientSession false
        let policy = 
            if isClientSession then
                AssemblyLookupPolicy.ResolveRuntime ||| 
                AssemblyLookupPolicy.ResolveVagabondCache ||| 
                AssemblyLookupPolicy.RequireLocalDependenciesLoadedInAppDomain
            else
                AssemblyLookupPolicy.ResolveRuntimeStrongNames ||| 
                AssemblyLookupPolicy.ResolveVagabondCache

        registerVagabond (not isClientSession) (fun () -> Vagabond.Initialize(ignoredAssemblies = [|Assembly.GetExecutingAssembly()|], ?cacheDirectory = workingDirectory, lookupPolicy = policy))