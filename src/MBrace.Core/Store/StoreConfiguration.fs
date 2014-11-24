namespace Nessos.MBrace.Store

open System.IO

/// Serializer abstraction
type ISerializer =
    inherit IResource

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


/// Store configuration
type CloudStoreConfiguration =
    {
        Serializer : ISerializer
        AtomProvider : ICloudAtomProvider option

        /// Default cloud file container for current execution contexts
        DefaultContainer : string
        FileProvider : ICloudFileProvider
    }