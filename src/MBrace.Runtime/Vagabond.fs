namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.Reflection
open System.IO

open Nessos.FsPickler
open Nessos.FsPickler.Json
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

/// Serializable FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AbstractClass; AutoSerializable(true)>]
type FsPicklerStoreSerializer () as self =
    // force exception in case of Vagabond instance not initialized
    do VagabondRegistry.Instance |> ignore

    // serializer instance registry for local AppDomain
    static let localInstances = new ConcurrentDictionary<string, FsPicklerSerializer> ()

    [<NonSerialized>]
    let mutable localInstance : FsPicklerSerializer option = None

    let getLocalInstance () =
        match localInstance with
        | Some instance -> instance
        | None ->
            // local instance not assigned, look up from registry
            let instance = localInstances.GetOrAdd(self.Id, ignore >> self.CreateLocalSerializerInstance)
            localInstance <- Some instance
            instance

    /// Gets the underlying, local FsPicklerSerializer instance being used
    member __.Serializer = getLocalInstance()

    abstract Id : string
    abstract CreateLocalSerializerInstance : unit -> FsPicklerSerializer

    interface ISerializer with
        member __.Id = __.Id
        member __.IsSerializable(value : 'T) = try FsPickler.EnsureSerializable value ; true with _ -> false
        member __.Serialize (target : Stream, value : 'T, leaveOpen : bool) = getLocalInstance().Serialize(target, value, leaveOpen = leaveOpen)
        member __.Deserialize<'T>(stream, leaveOpen) = getLocalInstance().Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member __.SeqSerialize(stream, values : 'T seq, leaveOpen) = getLocalInstance().SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member __.SeqDeserialize<'T>(stream, leaveOpen) = getLocalInstance().DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)
        member __.ComputeObjectSize<'T>(graph:'T) = getLocalInstance().ComputeSize graph
        member __.Clone(graph:'T) = FsPickler.Clone graph

/// Serializable binary FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type FsPicklerBinaryStoreSerializer () =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler binary serializer"
    override __.CreateLocalSerializerInstance () = 
        VagabondRegistry.Instance.Serializer :> _


/// Serializable xml FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type FsPicklerXmlStoreSerializer (?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler xml serializer"
    override __.CreateLocalSerializerInstance () = 
        FsPickler.CreateXmlSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent) :> _

/// Serializable json FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type FsPicklerJsonStoreSerializer (?omitHeader : bool, ?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler json serializer"
    override __.CreateLocalSerializerInstance () = 
        FsPickler.CreateJsonSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent, ?omitHeader = omitHeader) :> _