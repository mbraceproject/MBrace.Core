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


/// Global store registy; used for bootstrapping store connection settings on
/// data primitive deserialization.
type SerializerRegistry private () =
    static let registry = new System.Collections.Concurrent.ConcurrentDictionary<string, ISerializer> ()

    static member Register(serializer : ISerializer, ?force) = 
        if defaultArg force false then
            registry.AddOrUpdate(serializer.Id, serializer, fun _ _ -> serializer) |> ignore
        elif registry.TryAdd(serializer.Id, serializer) then ()
        else
            let msg = sprintf "SerializerRegistry: a serializer with id '%O' already exists in registry." id
            invalidOp msg

    static member Resolve(id : string) = 
        let mutable serializer = Unchecked.defaultof<ISerializer>
        if registry.TryGetValue(id, &serializer) then serializer
        else
            let msg = sprintf "SerializerRegistry: no serializer with id '%O' could be resolved." id
            invalidOp msg