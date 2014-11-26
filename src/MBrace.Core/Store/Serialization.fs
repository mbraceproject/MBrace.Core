namespace Nessos.MBrace.Store

open System
open System.IO

/// Serialization abstraction
type ISerializer =

    /// Serializer identifier
    abstract Id : string

    /// <summary>
    ///     Serializes a value to stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    /// <param name="value">Input value.</param>
    abstract Serialize<'T> : target:Stream * value:'T -> unit

    /// <summary>
    ///     Deserializes a value from stream.
    /// </summary>
    /// <param name="source">Source stream.</param>
    abstract Deserialize<'T> : source:Stream -> 'T

    /// <summary>
    ///     Serializes a sequence to stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    /// <param name="values">Input sequence.</param>
    /// <returns>Serialized element count.</returns>
    abstract SeqSerialize<'T> : target:Stream * values:seq<'T> -> int

    /// <summary>
    ///     Deserialize a sequence from stream.
    /// </summary>
    /// <param name="source">Source stream.</param>
    /// <param name="length">Expected number of elements.</param>
    abstract SeqDeserialize<'T> : source:Stream * length:int -> seq<'T>


/// Global serialization registy; used for bootstrapping data deserialization
/// in storage primitives
type SerializerRegistry private () =
    static let registry = new System.Collections.Concurrent.ConcurrentDictionary<string, ISerializer> ()

    /// <summary>
    ///     Registers a serializer instance.
    /// </summary>
    /// <param name="serializer">Serializer to be registered.</param>
    /// <param name="force">Force overwrite. Defaults to false.</param>
    static member Register(serializer : ISerializer, ?force : bool) : unit =
        if defaultArg force false then
            registry.AddOrUpdate(serializer.Id, serializer, fun _ _ -> serializer) |> ignore
        elif registry.TryAdd(serializer.Id, serializer) then ()
        else
            let msg = sprintf "SerializerRegistry: a serializer with id '%O' already exists in registry." id
            invalidOp msg

    /// <summary>
    ///     Resolves a registered serializer instance by id.
    /// </summary>
    /// <param name="id">Serializer id.</param>
    static member Resolve(id : string) : ISerializer = 
        let mutable serializer = Unchecked.defaultof<ISerializer>
        if registry.TryGetValue(id, &serializer) then serializer
        else
            let msg = sprintf "SerializerRegistry: no serializer with id '%O' could be resolved." id
            invalidOp msg