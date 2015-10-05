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
open System.Threading
open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
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

    [<RequireQualifiedAccess>]
    module Disposable =
        let inline dispose (d : IDisposable) = d.Dispose()
        let combine (ds : seq<#IDisposable>) =
            let ds = Seq.toArray ds
            { new IDisposable with
                member __.Dispose() = for d in ds do d.Dispose()
            }

    [<RequireQualifiedAccess>]
    module Array =
        
        /// <summary>
        ///     Filters array by predicate that takes the original element index as argument.
        /// </summary>
        /// <param name="f">Filter predicate.</param>
        /// <param name="ts">Input array.</param>
        let filteri (f : int -> 'T -> bool) (ts : 'T []) : 'T [] =
            let ra = new ResizeArray<'T> ()
            for i = 0 to ts.Length - 1 do
                let t = ts.[i]
                if f i t then ra.Add t

            ra.ToArray()
    
    let hset (xs : 'T seq) = new System.Collections.Generic.HashSet<'T>(xs)

    /// <summary>
    ///     Thread-safe memoization combinator.
    /// </summary>
    /// <param name="f">Input function.</param>
    let concurrentMemoize (f : 'T -> 'S) : ('T -> 'S) =
        let d = new ConcurrentDictionary<'T,'S>()
        fun t -> d.GetOrAdd(t, f)

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
        /// <summary>
        ///     Generates a working directory path that is unique to the current process
        /// </summary>
        /// <param name="prefix">Optional directory name prefix.</param>
        static member GetDefaultWorkingDirectoryForProcess(?prefix : string) : string =
            let prefix = defaultArg prefix "mbrace"
            Path.Combine(Path.GetTempPath(), sprintf "%s-pid%d" prefix <| Process.GetCurrentProcess().Id)

        /// <summary>
        ///     Generates a working directory path that is unique.
        /// </summary>
        /// <param name="prefix">Optional directory name prefix.</param>
        static member GetRandomWorkingDirectory(?prefix : string) : string =
            let prefix = defaultArg prefix "mbrace"
            let g = Guid.NewGuid()
            Path.Combine(Path.GetTempPath(), sprintf "%s-uuid%O" prefix g)

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

        [<OnDeserialized>]
        let _onDeserialized (_ : StreamingContext) =
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


    /// Serializable machine identifier object
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


    /// Serializable process identifier object
    [<Sealed; DataContract>]
    type ProcessId private (machineId : MachineId, processId : int, processName : string, startTime : DateTimeOffset) =
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
                    let cloudProcess = Process.GetProcessById(processId)
                    if processName = cloudProcess.ProcessName && startTime = new DateTimeOffset(cloudProcess.StartTime) then
                        Some cloudProcess
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
        /// <param name="cloudProcess">Process instance.</param>
        static member FromProcess(cloudProcess : System.Diagnostics.Process) : ProcessId =
            let mid = MachineId.LocalInstance
            new ProcessId(mid, cloudProcess.Id, cloudProcess.ProcessName, new DateTimeOffset(cloudProcess.StartTime))

        /// Gets the process identifier for the current process
        static member LocalInstance : ProcessId = singleton.Value

        /// <summary>
        ///     Gets a process identifier by local process id
        /// </summary>
        /// <param name="id">Process identifier.</param>
        static member TryGetProcessById(id : int) : ProcessId option =
            try id |> Process.GetProcessById |> ProcessId.FromProcess |> Some
            with :? ArgumentException -> None

    /// Serializable AppDomain identifier object
    [<Sealed; DataContract>]
    type AppDomainId private (processId : ProcessId, uuid : Guid, domainId : int, friendlyName : string) =
        static let localInstance = lazy(AppDomainId.FromAppDomain(AppDomain.CurrentDomain))

        [<DataMember(Name = "ProcessId")>]
        let processId = processId
        [<DataMember(Name = "UUID")>]
        let uuid = uuid
        [<DataMember(Name = "DomainId")>]
        let domainId = domainId
        [<DataMember(Name = "FriendlyName")>]
        let friendlyName = friendlyName

        /// Machine identifier
        member __.ProcessId = processId
        /// Unique AppDomain identifier
        member __.UUID = uuid
        /// AppDomain identifier
        member __.DomainId = domainId
        /// AppDomain friendly name
        member __.FriendlyName = friendlyName

        override __.Equals(other:obj) =
            match other with
            | :? AppDomainId as did -> processId = did.ProcessId && uuid = did.UUID
            | _ -> false

        override __.GetHashCode() = hash2 processId uuid

        interface IComparable with
            member __.CompareTo(other:obj) =
                match other with
                | :? AppDomainId as did -> compare2 processId uuid did.ProcessId did.UUID
                | _ -> invalidArg "other" "invalid comparand."

        /// Gets the identifier to current AppDomain
        static member LocalInstance = localInstance.Value

        /// <summary>
        ///     Gets a process identifier from given System.Diagnostics.process instance
        /// </summary>
        /// <param name="cloudProcess">Process instance.</param>
        static member FromAppDomain(domain : System.AppDomain) : AppDomainId =
            let pid = ProcessId.LocalInstance
            let uuid = AppDomainUUID.GetDomainId domain
            new AppDomainId(pid, uuid, domain.Id, domain.FriendlyName)


    and private AppDomainUUID private () =
        inherit MarshalByRefObject()
        static let uuid = Guid.NewGuid()
        let uuid = uuid
        member __.UUID = uuid
        static member GetDomainId(domain : AppDomain) =
            if domain = AppDomain.CurrentDomain then uuid
            else
                let t = typeof<AppDomainUUID>
                let ctorFlags = BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance
                let culture = System.Globalization.CultureInfo.CurrentCulture
                let handle = domain.CreateInstance(t.Assembly.FullName, t.FullName, false, ctorFlags, null, [||], culture, [||])
                let uuid = handle.Unwrap() :?> AppDomainUUID
                uuid.UUID

    type ISerializer with
        member s.Pickle<'T>(value : 'T) =
            use m = new MemoryStream()
            s.Serialize(m, value, leaveOpen = true)
            m.ToArray()

        member s.UnPickle<'T>(pickle : byte[]) =
            use m = new MemoryStream(pickle)
            s.Deserialize<'T>(m, leaveOpen = true)


    // MarshalledAction: used for sending cross-AppDomain events

    type private ActionProxy<'T>(action : 'T -> unit) =
        inherit MarshalByRefObject()
        override __.InitializeLifetimeService () = null
        member __.Trigger(pickle : byte[]) =
            try 
                let t = VagabondRegistry.Instance.Serializer.UnPickle<'T>(pickle)
                let _ = Async.StartAsTask(async { action t })
                null
            with e ->
                VagabondRegistry.Instance.Serializer.Pickle e

        member self.Disconnect() =
            ignore <| System.Runtime.Remoting.RemotingServices.Disconnect self

    /// Action that can be marshalled across AppDomains
    [<Sealed; DataContract>]
    type MarshaledAction<'T> internal (action : 'T -> unit) =
        do VagabondRegistry.Instance |> ignore
        [<IgnoreDataMember>]
        let mutable proxy = new ActionProxy<'T>(action)
        [<DataMember(Name = "ObjRef")>]
        let objRef = System.Runtime.Remoting.RemotingServices.Marshal(proxy)
        [<OnDeserialized>]
        let _onDeserialized (_ : StreamingContext) =
            proxy <- System.Runtime.Remoting.RemotingServices.Unmarshal objRef :?> ActionProxy<'T>

        /// Invokes the marshaled action with supplied argument
        member __.Invoke(t : 'T) =
            let pickle = VagabondRegistry.Instance.Serializer.Pickle t
            match proxy.Trigger pickle with
            | null -> ()
            | exnP -> raise <| VagabondRegistry.Instance.Serializer.UnPickle<exn> exnP

        /// Disposes the marshaled action
        member __.Dispose() = proxy.Disconnect()

    and MarshaledAction =
        /// <summary>
        ///     Creates an action that can be serialized across AppDomains.
        /// </summary>
        /// <param name="action">Action to be marshalled.</param>
        static member Create(action : 'T -> unit) = new MarshaledAction<_>(action)


    type Thread with
        /// <summary>
        ///     Computation that diverges on the current thread.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for diverging computation.</param>
        static member Diverge<'T>(?cancellationToken : CancellationToken) : 'T =
            let cts = new TaskCompletionSource<'T>()
            cancellationToken |> Option.iter (fun ct -> ignore <| ct.Register(fun () -> ignore <| cts.TrySetCanceled()))
            cts.Task.Result