namespace MBrace.Runtime.Utils

open System
open System.Reflection
open System.IO
open System.Diagnostics
open System.Net
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading.Tasks
open System.Text
open System.Text.RegularExpressions

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry

open Nessos.FsPickler.Hashing

#nowarn "444"

[<AutoOpen>]
module Utils =

    let runsOnMono = System.Type.GetType("Mono.Runtime") <> null

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

    [<RequireQualifiedAccess>]
    module Exn =

        let inline catch (f : unit -> 'T) =
            try f () |> Success with e -> Error e

        let inline protect f t = try f t |> Success with e -> Error e
        let inline protect2 f t s = try f t s |> Success with e -> Error e

        let inline get (e : Exn<'T>) : 'T = e.Value

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

    /// lexicographic comparison without tuple allocation
    let inline compare2 (t1 : 'T) (s1 : 'S) (t2 : 'T) (s2 : 'S) =
        match compare t1 t2 with
        | 0 -> compare s1 s2
        | x -> x

    /// lexicographic comparison without tuple allocation
    let inline compare3 (t1 : 'T) (s1 : 'S) (u1 : 'U) (t2 : 'T) (s2 : 'S) (u2 : 'U) =
        match compare t1 t2 with
        | 0 ->
            match compare s1 s2 with
            | 0 -> compare u1 u2
            | x -> x
        | x -> x

    /// lexicographic comparison without tuple allocation
    let inline compare4 (t1 : 'T) (s1 : 'S) (u1 : 'U) (v1 : 'V) (t2 : 'T) (s2 : 'S) (u2 : 'U) (v2 : 'V) =
        match compare t1 t2 with
        | 0 ->
            match compare s1 s2 with
            | 0 -> 
                match compare u1 u2 with
                | 0 -> compare v1 v2
                | x -> x
            | x -> x
        | x -> x

    
    /// taken from mscorlib's Tuple.GetHashCode() implementation
    let inline private combineHash (h1 : int) (h2 : int) =
        ((h1 <<< 5) + h1) ^^^ h2

    /// pair hashcode generation without tuple allocation
    let inline hash2 (t : 'T) (s : 'S) =
        combineHash (hash t) (hash s)
        
    /// triple hashcode generation without tuple allocation
    let inline hash3 (t : 'T) (s : 'S) (u : 'U) =
        combineHash (combineHash (hash t) (hash s)) (hash u)

    /// quadruple hashcode generation without tuple allocation
    let inline hash4 (t : 'T) (s : 'S) (u : 'U) (v : 'V) =
        combineHash (combineHash (combineHash (hash t) (hash s)) (hash u)) (hash v)

    /// reflection-based equality check
    /// checks if args are of identical underlying type before checking equality
    /// used to protect against incomplete equality implementations in interfaces
    /// that demand equality
    let inline areReflectiveEqual<'T when 'T : not struct and 'T : equality> (x : 'T) (y : 'T) =
        if obj.ReferenceEquals(x, null) <> obj.ReferenceEquals(y, null) then false
        elif x.GetType() <> y.GetType() then false
        else x = y

    /// Gets the actual System.Type of the underlying object
    let inline getReflectedType (o:obj) =
        if obj.ReferenceEquals(o, null) then typeof<obj>
        else o.GetType()
            

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

    type ICloudLogger with
        member inline l.Logf fmt = Printf.ksprintf l.Log fmt

    type IDictionary<'K,'V> with
        member d.TryFind(k : 'K) : 'V option =
            let mutable v = Unchecked.defaultof<'V>
            if d.TryGetValue(k, &v) then Some v else None
    
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


    /// MailboxProcessor ReplyChannel with exception support
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


    type ReferenceEqualityComparer<'T> () =
        interface IEqualityComparer<'T> with
            member __.Equals(x,y) = obj.ReferenceEquals(x,y)
            member __.GetHashCode x = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode x

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

    /// A Serializable value container that only serializes its factory
    /// rather than the actual value.
    [<DataContract; Sealed>]
    type SerializableFactory<'T> private (factory : unit -> 'T) =
        [<DataMember(Name = "Factory")>]
        let factory = factory
        [<IgnoreDataMember>]
        let mutable value = factory ()

        member private __.OnDeserialized (_ : StreamingContext) =
            value <- factory()

        member __.Value = value

        static member internal Init(factory : unit -> 'T) = new SerializableFactory<'T>(factory)

    /// A Serializable value container that only serializes its factory
    /// rather than the actual value.
    and SerializableFactory =
        /// <summary>
        ///     Initializes a serializable factory container.
        /// </summary>
        /// <param name="factory">Factory to be initialized.</param>
        static member Init(factory : unit -> 'T) = SerializableFactory<'T>.Init(factory)


    /// Unique machine identifier object
    [<Sealed; DataContract>]
    type MachineId private (hostname : string, interfaces : string []) =
        static let mkLocal() =
            let hostname = Dns.GetHostName()
            let interfaces = Dns.GetHostAddresses(hostname) |> Array.map (fun i -> i.ToString())
            new MachineId(hostname, interfaces)

        static let localSingleton = lazy(mkLocal())

        [<DataMember(Name = "Hostname")>]
        let hostname = hostname
        [<DataMember(Name = "Interfaces")>]
        let interfaces = interfaces

        /// Hostname of machine
        member __.Hostname = hostname
        member private __.Interfaces = interfaces

        override __.Equals(other:obj) =
            match other with
            | :? MachineId as mid -> hostname = mid.Hostname && interfaces = mid.Interfaces
            | _ -> false

        override __.GetHashCode() = hash2 hostname interfaces

        interface IComparable with
            member __.CompareTo(other:obj) =
                match other with
                | :? MachineId as mid -> compare2 hostname interfaces mid.Hostname mid.Interfaces
                | _ -> invalidArg "other" "invalid comparand."

        /// Gets the local instance identifier
        static member LocalInstance = localSingleton.Value


    /// Unique process identifier object
    [<Sealed; DataContract>]
    type ProcessId private (machineId : MachineId, processId : int, processName : string, startTime : DateTime) =
        static let singleton = lazy(ProcessId.FromProcess <| Process.GetCurrentProcess())

        [<DataMember(Name = "MachineId")>]
        let machineId = machineId
        [<DataMember(Name = "Id")>]
        let processId = processId
        [<DataMember(Name = "Name")>]
        let processName = processName
        [<DataMember(Name = "StartTime")>]
        let startTime = startTime

        /// Machine identifier
        member __.MachineId = machineId
        /// Process Identifier
        member __.Id = processId
        /// Process name
        member __.Name = processName
        /// Process start time
        member __.StartTime = startTime

        /// Gets a System.Diagnostics.Process instance that corresponds to the
        /// ProcessId provided that it is still running.
        member __.TryGetLocalProcess() =
            if machineId = MachineId.LocalInstance then
                try 
                    let proc = Process.GetProcessById(processId)
                    if processName = proc.ProcessName && startTime = proc.StartTime then
                        Some proc
                    else
                        None
                with :? ArgumentException -> None
            else
                None

        override __.Equals(other:obj) =
            match other with
            | :? ProcessId as pid -> machineId = pid.MachineId && processId = pid.Id && processName = pid.Name && startTime = pid.StartTime
            | _ -> false

        override __.GetHashCode() = hash4 machineId processId processName startTime

        interface IComparable with
            member __.CompareTo(other:obj) =
                match other with
                | :? ProcessId as pid -> compare4 machineId processId processName startTime pid.MachineId pid.Id pid.Name pid.StartTime
                | _ -> invalidArg "other" "invalid comparand."

        /// <summary>
        ///     Gets a process identifier from given System.Diagnostics.process instance
        /// </summary>
        /// <param name="proc">Process instance.</param>
        static member FromProcess(proc : System.Diagnostics.Process) : ProcessId =
            let mid = MachineId.LocalInstance
            new ProcessId(mid, proc.Id, proc.ProcessName, proc.StartTime)

        /// Gets the process identifier for the current process
        static member LocalInstance : ProcessId = singleton.Value

        /// <summary>
        ///     Gets a process identifier by local process id
        /// </summary>
        /// <param name="id">Process identifier.</param>
        static member TryGetProcessById(id : int) : ProcessId option =
            try id |> Process.GetProcessById |> ProcessId.FromProcess |> Some
            with :? ArgumentException -> None


    type ISerializer with
        member s.Pickle<'T>(value : 'T) =
            use m = new MemoryStream()
            s.Serialize(m, value, leaveOpen = true)
            m.ToArray()

        member s.UnPickle<'T>(pickle : byte[]) =
            use m = new MemoryStream(pickle)
            s.Deserialize<'T>(m, leaveOpen = true)