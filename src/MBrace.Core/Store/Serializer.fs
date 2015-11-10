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


/// Text-based serialization abstraction
type ITextSerializer =
    inherit ISerializer

    /// <summary>
    ///     Serializes a value to text writer
    /// </summary>
    /// <param name="target">Target text writer.</param>
    /// <param name="value">Value to be serialized.</param>
    /// <param name="leaveOpen">Leave open text writer after serialization.</param>
    abstract TextSerialize<'T> : target:TextWriter * value:'T * leaveOpen : bool -> unit

    /// <summary>
    ///     Deserializes a value from text reader
    /// </summary>
    /// <param name="source">Source text reader.</param>
    /// <param name="leaveOpen">Leave open text reader after deserialization.</param>
    abstract TextDeserialize<'T> : source:TextReader * leaveOpen : bool -> 'T


namespace MBrace.Core

open System
open System.IO
open System.Text

open MBrace.Core.Internals

#nowarn "444"

/// Common MBrace serialization methods
type Serializer private () =
    
    static let getTextSerializer () = local {
        let! serializer = Cloud.TryGetResource<ISerializer>()
        match serializer with
        | Some (:? ITextSerializer as ts) -> return ts
        | _ -> return! Cloud.GetResource<ITextSerializer>()
    }

    /// <summary>
    ///     Quickly computes the size of a serializable object graph in bytes.
    /// </summary>
    /// <param name="graph">Serializable object graph to be computed.</param>
    static member ComputeObjectSize<'T>(graph : 'T) : LocalCloud<int64> = local {
        let! serializer = Cloud.GetResource<ISerializer> ()
        return serializer.ComputeObjectSize graph
    }

    /// <summary>
    ///     Creates an in-memory clone of supplied serializable object graph.
    /// </summary>
    /// <param name="graph">Serializable object graph to be cloned.</param>
    static member Clone<'T>(graph : 'T) : LocalCloud<'T> = local {
        let! serializer = Cloud.GetResource<ISerializer> ()
        return serializer.Clone graph
    }

    /// <summary>
    ///     Serializes provided object graph to underlying write stream.
    /// </summary>
    /// <param name="stream">Stream to serialize object.</param>
    /// <param name="graph">Object graph to be serialized.</param>
    /// <param name="leaveOpen">Leave open stream after serialization. Defaults to false.</param>
    static member Serialize<'T>(stream : Stream, graph : 'T, [<O;D(null:obj)>]?leaveOpen : bool) : LocalCloud<unit> = local {
        let leaveOpen = defaultArg leaveOpen false
        let! serializer = Cloud.GetResource<ISerializer> ()
        return serializer.Serialize(stream, graph, leaveOpen)
    }

    /// <summary>
    ///     Deserializes provided object graph from underlying read stream.
    /// </summary>
    /// <param name="stream">Stream to deserialize object from.</param>
    /// <param name="leaveOpen">Leave open stream after deserialization. Defaults to false.</param>
    static member Deserialize<'T>(stream : Stream, [<O;D(null:obj)>]?leaveOpen : bool) : LocalCloud<'T> = local {
        let leaveOpen = defaultArg leaveOpen false
        let! serializer = Cloud.GetResource<ISerializer> ()
        return serializer.Deserialize<'T>(stream, leaveOpen)
    }

    /// <summary>
    ///     Serializes provided object graph to underlying text writer.
    /// </summary>
    /// <param name="target">Target text writer.</param>
    /// <param name="graph">Object graph to be serialized.</param>
    /// <param name="leaveOpen">Leave open writer after serialization. Defaults to false.</param>
    static member TextSerialize<'T>(target : TextWriter, graph : 'T, [<O;D(null:obj)>]?leaveOpen : bool) : LocalCloud<unit> = local {
        let leaveOpen = defaultArg leaveOpen false
        let! ts = getTextSerializer()
        return ts.TextSerialize<'T>(target, graph, leaveOpen)
    }

    /// <summary>
    ///     Deserializes object graph from underlying text reader.
    /// </summary>
    /// <param name="source">Source text reader.</param>
    /// <param name="leaveOpen">Leave open writer after deserialization. Defaults to false.</param>
    static member TextDeserialize<'T>(source : TextReader, [<O;D(null:obj)>]?leaveOpen : bool) : LocalCloud<'T> = local {
        let leaveOpen = defaultArg leaveOpen false
        let! ts = getTextSerializer()
        return ts.TextDeserialize<'T>(source, leaveOpen)
    }

    /// <summary>
    ///     Serializes provided object graph to byte array.
    /// </summary>
    /// <param name="graph">Object graph to be serialized.</param>
    static member Pickle<'T>(graph : 'T) : LocalCloud<byte []> = local {
        let! serializer = Cloud.GetResource<ISerializer> ()
        use mem = new MemoryStream()
        do serializer.Serialize(mem, graph, true)
        return mem.ToArray()
    }

    /// <summary>
    ///     Deserializes object from given byte array pickle.
    /// </summary>
    /// <param name="pickle">Input serialization bytes.</param>
    static member UnPickle<'T>(pickle : byte[]) : LocalCloud<'T> = local {
        let! serializer = Cloud.GetResource<ISerializer> ()
        use mem = new MemoryStream(pickle)
        return serializer.Deserialize<'T>(mem, true)
    }

    /// <summary>
    ///     Serializes provided object graph to string.
    /// </summary>
    /// <param name="graph">Graph to be serialized.</param>
    static member PickleToString<'T>(graph : 'T) = local {
        let! serializer = getTextSerializer()
        use sw = new StringWriter()
        serializer.TextSerialize(sw, graph, true)
        return sw.ToString()
    }

    /// <summary>
    ///     Deserializes object from given string pickle.
    /// </summary>
    /// <param name="pickle">Input serialization string.</param>
    static member UnPickleOfString<'T>(pickle : string) : LocalCloud<'T> = local {
        let! serializer = getTextSerializer()
        use sr = new StringReader(pickle)
        return serializer.TextDeserialize<'T>(sr, true)   
    }