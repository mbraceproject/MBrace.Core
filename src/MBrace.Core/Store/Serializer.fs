namespace MBrace.Core.Internals

open System
open System.IO

open System
open System.IO

/// Stateful instance used for counting sizes of streams
type IObjectSizeCounter<'T> =
    inherit IDisposable
    /// Append object graph to count.
    abstract Append : graph:'T -> unit
    /// Gets the total number of appended objects.
    abstract TotalObjects : int64
    /// Gets the total number of counted bytes.
    abstract TotalBytes : int64

/// Serialization abstraction
type ISerializer =

    /// Serializer identifier
    abstract Id : string

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
    ///     Creates a typed object counter instance.
    /// </summary>
    abstract CreateObjectSizeCounter<'T> : unit -> IObjectSizeCounter<'T>