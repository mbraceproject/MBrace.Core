namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

/// Partition a seq<'T> to seq<seq<'T>> using a predicate
type private PartitionedEnumerable<'T> private (splitNext : unit -> bool, source : IEnumerable<'T>) = 
    let e = source.GetEnumerator()
    let mutable sourceMoveNext = true

    let innerEnumerator =
        { new IEnumerator<'T> with
            member __.MoveNext() : bool = 
                if splitNext() then false
                else
                    sourceMoveNext <- e.MoveNext()
                    sourceMoveNext

            member __.Current : obj = e.Current  :> _
            member __.Current : 'T = e.Current
            member __.Dispose() : unit = () 
            member __.Reset() : unit = invalidOp "Reset" }

    let innerSeq = 
        { new IEnumerable<'T> with
                member __.GetEnumerator() : IEnumerator = innerEnumerator :> _
                member __.GetEnumerator() : IEnumerator<'T> = innerEnumerator }

    let outerEnumerator =
        { new IEnumerator<IEnumerable<'T>> with
                member __.Current: IEnumerable<'T> = innerSeq
                member __.Current: obj = innerSeq :> _
                member __.Dispose(): unit = ()
                member __.MoveNext() = sourceMoveNext
                member __.Reset(): unit = invalidOp "Reset"
        }

    interface IEnumerable<IEnumerable<'T>> with
        member this.GetEnumerator() : IEnumerator = outerEnumerator :> _
        member this.GetEnumerator() : IEnumerator<IEnumerable<'T>> = outerEnumerator :> _ 

    static member ofSeq (splitNext : unit -> bool) (source : seq<'T>) : seq<seq<'T>> =
        new PartitionedEnumerable<'T>(splitNext, source) :> _