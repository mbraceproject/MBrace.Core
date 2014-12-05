namespace Nessos.MBrace.Store

open System
open System.IO

open System
open System.IO

/// Serialization abstraction
type ISerializer =

    /// Serializer identifier
    abstract Id : string

    /// Creates a serializable descriptor used for 
    /// re-establishing serializer instances in remote processes
    abstract GetSerializerDescriptor : unit -> ISerializerDescriptor

    /// <summary>
    ///     Serializes a value to stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    /// <param name="value">Input value.</param>
    abstract Serialize<'T> : target:Stream * value:'T * leaveOpen:bool -> unit

    /// <summary>
    ///     Deserializes a value from stream.
    /// </summary>
    /// <param name="source">Source stream.</param>
    abstract Deserialize<'T> : source:Stream * leaveOpen:bool -> 'T

    /// <summary>
    ///     Serializes a sequence to stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    /// <param name="values">Input sequence.</param>
    /// <returns>Serialized element count.</returns>
    abstract SeqSerialize<'T> : target:Stream * values:seq<'T> * leaveOpen:bool -> int

    /// <summary>
    ///     Deserialize a sequence from stream.
    /// </summary>
    /// <param name="source">Source stream.</param>
    /// <param name="length">Expected number of elements.</param>
    abstract SeqDeserialize<'T> : source:Stream * length:int * leaveOpen:bool -> seq<'T>

/// Serializable serializer identifier
/// that can be recovered in remote processes.
and ISerializerDescriptor =
    /// Descriptor Identifier
    abstract Id : string
    /// Recovers the serializer instance locally
    abstract Recover : unit -> ISerializer