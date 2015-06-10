namespace MBrace.Runtime.Vagabond

open System
open System.Diagnostics
open System.Reflection
open System.IO

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Runtime.Utils

/// Vagabond state container
type VagabondRegistry private () =

    static let lockObj = obj()
    static let mutable instance : VagabondManager option = None

    /// Gets the registered vagabond instance.
    static member Instance =
        match instance with
        | None -> invalidOp "No instance of vagabond has been registered."
        | Some instance -> instance

    /// <summary>
    ///     Initializes the registry using provided factory.
    /// </summary>
    /// <param name="factory">Vagabond instance factory.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize(factory : unit -> VagabondManager, ?throwOnError) =
        lock lockObj (fun () ->
            match instance with
            | None -> instance <- Some <| factory ()
            | Some _ when defaultArg throwOnError true -> invalidOp "An instance of Vagabond has already been registered."
            | Some _ -> ())

    /// <summary>
    ///     Initializes vagabond using default settings.
    /// </summary>
    /// <param name="cachePath">Vagrant cache path.</param>
    /// <param name="ignoreAssembly">Specify an optional ignore assembly predicate.</param>
    /// <param name="lookupPolicy">Specify a default assembly load policy.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    /// <param name="cleanup">Cleanup vagrant cache directory. Defaults to false.</param>
    static member Initialize (?cachePath : string, ?ignoredAssemblies : seq<Assembly>, ?lookupPolicy, ?throwOnError : bool, ?cleanup : bool) =
        let ignoredAssemblies = seq { 
            yield Assembly.GetExecutingAssembly() 
            match ignoredAssemblies with 
            | None -> () 
            | Some ias -> yield! ias
        }

        let cachePath =
            match cachePath with
            | Some cp -> cp
            | None -> Path.Combine(WorkingDirectory.GetDefaultWorkingDirectoryForProcess(), "vagabond")

        VagabondRegistry.Initialize((fun () ->
            WorkingDirectory.CreateWorkingDirectory(cachePath, cleanup = defaultArg cleanup false)
            Vagabond.Initialize(cacheDirectory = cachePath, ignoredAssemblies = ignoredAssemblies, ?lookupPolicy = lookupPolicy)), ?throwOnError = throwOnError)