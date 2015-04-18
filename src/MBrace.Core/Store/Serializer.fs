namespace MBrace.Store.Internals

open System
open System.IO

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

[<AutoOpen>]
module SerializerUtils =
    
    type ISerializer with
        /// <summary>
        ///     Serializes value to byte array
        /// </summary>
        /// <param name="value">Input value.</param>
        member s.Pickle<'T>(value : 'T) : byte [] =
            use m = new MemoryStream()
            s.Serialize(m, value, false)
            m.ToArray()

        /// <summary>
        ///     Deserializes value from byte array.
        /// </summary>
        /// <param name="pickle">Input serialization</param>
        member s.UnPickle<'T>(pickle : byte []) : 'T =
            use m = new MemoryStream(pickle)
            s.Deserialize<'T>(m, false)