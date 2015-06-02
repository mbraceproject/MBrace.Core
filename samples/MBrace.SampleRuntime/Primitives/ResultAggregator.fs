namespace MBrace.SampleRuntime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Runtime

type private ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * overwrite:bool * completed:IReplyChannel<bool>
    | GetCompleted of IReplyChannel<int>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

/// A distributed resource that aggregates an array of results.
type ResultAggregator<'T> private (capacity : int, source : ActorRef<ResultAggregatorMsg<'T>>) =
    interface IResultAggregator<'T> with
        member __.Capacity = capacity
        member __.CurrentSize = source <!- GetCompleted
        member __.IsCompleted = source <!- IsCompleted
        member __.SetResult(index : int, value : 'T, overwrite : bool) = source <!- fun ch -> SetResult(index, value, overwrite, ch)
        member __.ToArray () = source <!- ToArray
        member __.Dispose () = async.Zero()

    /// Initializes a result aggregator of given size at the current process.
    static member Init(size : int) =
        let behaviour (results : Map<int, 'T>) msg = async {
            match msg with
            | SetResult(i, value, _, rc) when i < 0 || i >= size ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, false, rc) when results.ContainsKey i ->
                let e = new InvalidOperationException(sprintf "result at position '%d' has already been set." i)
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, _, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = size
                do! rc.Reply isCompleted
                return results

            | GetCompleted rc ->
                do! rc.Reply results.Count
                return results

            | IsCompleted rc ->
                do! rc.Reply ((results.Count = size))
                return results

            | ToArray rc when results.Count = size ->
                let array = results |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray
                do! rc.Reply array
                return results

            | ToArray rc ->
                let e = new InvalidOperationException("Result aggregator incomplete.")
                do! rc.ReplyWithException e
                return results
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new ResultAggregator<'T>(size, ref)