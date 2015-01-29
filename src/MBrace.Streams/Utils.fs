namespace MBrace.Streams

open System
open System.Collections
open System.Collections.Generic

/// Helper type to partition a seq<'T> to seq<seq<'T>> using a predicate
type PartitionedEnumerable<'T> private (predicate : unit -> bool, source : IEnumerable<'T>) = 
    let e = source.GetEnumerator()
    let mutable sourceMoveNext = true

    let innerEnumerator =
        { new IEnumerator<'T> with
            member __.MoveNext() : bool = 
                if predicate() then 
                    sourceMoveNext <- e.MoveNext()
                    sourceMoveNext
                else false
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

    static member ofSeq (predicate : unit -> bool) (source : seq<'T>) : seq<seq<'T>> =
        new PartitionedEnumerable<'T>(predicate, source) :> _
