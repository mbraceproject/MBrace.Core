namespace MBrace.Core.Internals

open System
open System.IO

open System
open System.IO

/// Serialization abstraction
type ISerializer =

    /// Serializer identifier
    abstract Id : string

    /// <summary>
    ///     Checks if supplied value can be serialized.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSerializable<'T> : value:'T -> bool

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
    ///     Lazily serializes a sequence to stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    /// <param name="values">Input sequence.</param>
    /// <returns>Serialized element count.</returns>
    abstract SeqSerialize<'T> : target:Stream * values:seq<'T> * leaveOpen:bool -> int

    /// <summary>
    ///     Lazily deserialize a sequence from stream.
    /// </summary>
    /// <param name="source">Source stream.</param>
    abstract SeqDeserialize<'T> : source:Stream * leaveOpen:bool -> seq<'T>

    /// <summary>
    ///     Computes serialization size of provided object graph in bytes.
    /// </summary>
    /// <param name="graph">Serializable object graph.</param>
    abstract ComputeObjectSize<'T> : graph:'T -> int64

    /// <summary>
    ///     Creates a cloned copy of a serializable object graph.
    /// </summary>
    /// <param name="graph">Object graph to be cloned.</param>
    abstract Clone : graph:'T -> 'T