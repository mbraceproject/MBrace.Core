namespace MBrace.Runtime.Utils

open System
open System.Reflection
open System.IO
open System.Diagnostics
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry

#nowarn "444"

[<AutoOpen>]
module Utils =

    /// Value or exception
    [<NoEquality; NoComparison>]
    type Exn<'T> =
        | Success of 'T
        | Error of exn
    with
        /// evaluate, re-raising the exception if failed
        member inline e.Value =
            match e with
            | Success t -> t
            | Error e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e

    module Exn =
        let inline catch (f : unit -> 'T) =
            try f () |> Success with e -> Error e

        let inline protect f t = try f t |> Success with e -> Error e
        let inline protect2 f t s = try f t s |> Success with e -> Error e

        let map (f : 'T -> 'S) (x : Exn<'T>) =
            match x with
            | Success x -> Success (f x)
            | Error e -> Error e

        let bind (f : 'T -> 'S) (x : Exn<'T>) =
            match x with
            | Success x -> try Success <| f x with e -> Error e
            | Error e -> Error e


    [<RequireQualifiedAccess>]
    module Option =
        let toNullable(x : 'T option) =
            match x with
            | None -> new Nullable<'T>()
            | Some x -> new Nullable<'T>(x)
    
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

    type Async with
        static member OfCloud(workflow : Local<'T>, ?resources : ResourceRegistry) = async {
            let! ct = Async.CancellationToken
            let cct = new InMemoryRuntime.InMemoryCancellationToken(ct)
            let resources = defaultArg resources ResourceRegistry.Empty
            return! Cloud.ToAsync(workflow, resources, cct)
        }

    module MBraceAsyncExtensions =
        
        type AsyncBuilder with
            member inline ab.Bind(workflow : Local<'T>, f : 'T -> Async<'S>) = ab.Bind(Async.OfCloud(workflow), f)

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

    type ICloudLogger with
        member inline l.Logf fmt = Printf.ksprintf l.Log fmt
    
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
        static member CreateWorkingDirectory(?path : string, ?retries : int, ?cleanup : bool) : string =
            let path = match path with Some p -> p | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess()
            let retries = defaultArg retries 2
            let cleanup = defaultArg cleanup true
            retry (RetryPolicy.Retry(retries, 0.2<sec>)) 
                (fun () ->
                    if Directory.Exists path then
                        if cleanup then 
                            Directory.Delete(path, true)
                            if Directory.Exists path then
                                raise <| new IOException(sprintf "Could not delete directory '%s'." path)

                            ignore <| Directory.CreateDirectory path
                            if not <| Directory.Exists path then
                                raise <| new IOException(sprintf "Could not create directory '%s'." path)
                    else
                        ignore <| Directory.CreateDirectory path
                        if not <| Directory.Exists path then
                           raise <| new IOException(sprintf "Could not create directory '%s'." path))
            path


    type ReplyChannel<'T> internal (rc : AsyncReplyChannel<Exn<'T>>) =
        member __.Reply (t : 'T) = rc.Reply <| Success t
        member __.Reply (t : Exn<'T>) = rc.Reply t
        member __.ReplyWithError (e : exn) = rc.Reply <| Error e

    and MailboxProcessor<'T> with
        member m.PostAndAsyncReply (msgB : ReplyChannel<'R> -> 'T) = async {
            let! result = m.PostAndAsyncReply(fun ch -> msgB(new ReplyChannel<_>(ch)))
            return result.Value
        }

        member m.PostAndReply (msgB : ReplyChannel<'R> -> 'T) =
            m.PostAndAsyncReply msgB |> Async.RunSync


    /// Type existential container
    [<AbstractClass>]
    type Existential internal () =
        /// System.Type representation of type
        abstract Type : Type
        /// Accepts a generic thunk to encapsulated type
        abstract Apply<'R> : IFunc<'R> -> 'R
        /// Accepts a generic asynchronous thunk to encapsulated type
        abstract Apply<'R> : IAsyncFunc<'R> -> Async<'R>

        /// <summary>
        ///     Use reflection to initialize an encapsulated type.
        /// </summary>
        /// <param name="t"></param>
        static member FromType(t : Type) =
            let et = typedefof<Existential<_>>.MakeGenericType([|t|])
            let ctor = et.GetConstructor [||]
            ctor.Invoke [||] :?> Existential

    /// Existential container of type 'T
    and [<Sealed; AutoSerializable(true)>] Existential<'T> () =
        inherit Existential()

        override e.Type = typeof<'T>
        override e.Apply<'R> (f : IFunc<'R>) = f.Invoke<'T> ()
        override e.Apply<'R> (f : IAsyncFunc<'R>) = f.Invoke<'T> ()
        override e.Equals(other:obj) =
            match other with
            | :? Existential<'T> -> true
            | _ -> false

        override e.GetHashCode() = typeof<'T>.GetHashCode()

    /// Generic function
    and IFunc<'R> =
        abstract Invoke<'T> : unit -> 'R

    /// Generic asynchronous function
    and IAsyncFunc<'R> =
        abstract Invoke<'T> : unit -> Async<'R>