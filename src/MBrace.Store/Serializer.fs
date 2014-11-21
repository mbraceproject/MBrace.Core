namespace Nessos.MBrace.Store

open System.IO

type ISerializer =
    inherit IResource

    abstract Serialize : Stream * 'T -> unit
    abstract Deserialize : Stream -> 'T
    abstract SeqSerialize : Stream * seq<'T> -> int64
    abstract SeqDeserialize : Stream * int64 -> seq<'T>