namespace MBrace.Runtime.Utils

open System.IO
open System.Diagnostics
open System.Collections.Concurrent
open System.Threading.Tasks

open MBrace.Runtime.Utils.Retry

[<AutoOpen>]
module Utils =
    
    let hset (xs : 'T seq) = new System.Collections.Generic.HashSet<'T>(xs)

    /// generates a human readable string for byte sizes
    /// including a KiB, MiB, GiB or TiB suffix depending on size
    let getHumanReadableByteSize (size : int64) =
        if size <= 512L then sprintf "%d bytes" size
        elif size <= 512L * 1024L then sprintf "%.2f KiB" (decimal size / decimal 1024L)
        elif size <= 512L * 1024L * 1024L then sprintf "%.2f MiB" (decimal size / decimal (1024L * 1024L))
        elif size <= 512L * 1024L * 1024L * 1024L then sprintf "%.2f GiB" (decimal size / decimal (1024L * 1024L * 1024L))
        else sprintf "%.2f TiB" (decimal size / decimal (1024L * 1024L * 1024L * 1024L))

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)


    type ConcurrentDictionary<'K,'V> with
        member dict.TryAdd(key : 'K, value : 'V, ?forceUpdate) =
            if defaultArg forceUpdate false then
                let _ = dict.AddOrUpdate(key, value, fun _ _ -> value)
                true
            else
                dict.TryAdd(key, value)

    type Event<'T> with
        member e.TriggerAsTask(t : 'T) =
            System.Threading.Tasks.Task.Factory.StartNew(fun () -> e.Trigger t)

    
    type WorkingDirectory =
        /// Generates a working directory path that is unique to the current process
        static member GetDefaultWorkingDirectoryForProcess() : string =
            Path.Combine(Path.GetTempPath(), sprintf "mbrace-process-%d" <| Process.GetCurrentProcess().Id)

        /// <summary>
        ///     Creates a working directory suitable for the current process.
        /// </summary>
        /// <param name="path">Path to working directory. Defaults to default process-bound working directory.</param>
        /// <param name="retries">Retries on creating directory. Defaults to 3.</param>
        /// <param name="cleanup">Cleanup the working directory if it exists. Defaults to true.</param>
        static member CreateWorkingDirectory(?path : string, ?retries : int, ?cleanup : bool) =
            let path = match path with Some p -> p | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess()
            let retries = defaultArg retries 2
            let cleanup = defaultArg cleanup true
            retry (RetryPolicy.Retry(retries, 0.2<sec>)) 
                (fun () ->
                    if Directory.Exists path then
                        if cleanup then 
                            Directory.Delete(path, true)
                            ignore <| Directory.CreateDirectory path
                    else
                        ignore <| Directory.CreateDirectory path)