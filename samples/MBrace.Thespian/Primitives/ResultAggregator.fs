namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Runtime

[<AutoOpen>]
module private ActorResultAggregator =

    type ResultAggregatorMsg =
        | SetResult of index:int * pvalue:byte[] * overwrite:bool * completed:IReplyChannel<bool>
        | GetCompleted of IReplyChannel<int>
        | IsCompleted of IReplyChannel<bool>
        | ToArray of IReplyChannel<byte [][]>

    /// <summary>
    ///     Creates a result aggregator actor in the local process.
    /// </summary>
    /// <param name="capacity"></param>
    let create (capacity : int) : ActorRef<ResultAggregatorMsg> =
        let behaviour (results : Map<int, byte[]>) msg = async {
            match msg with
            | SetResult(i, value, _, rc) when i < 0 || i >= capacity ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, false, rc) when results.ContainsKey i ->
                let e = new InvalidOperationException(sprintf "result at position '%d' has already been set." i)
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, _, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = capacity
                do! rc.Reply isCompleted
                return results

            | GetCompleted rc ->
                do! rc.Reply results.Count
                return results

            | IsCompleted rc ->
                do! rc.Reply ((results.Count = capacity))
                return results

            | ToArray rc when results.Count = capacity ->
                let array = results |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray
                do! rc.Reply array
                return results

            | ToArray rc ->
                let e = new InvalidOperationException("Result aggregator incomplete.")
                do! rc.ReplyWithException e
                return results
        }

        Actor.Stateful Map.empty behaviour
        |> Actor.Publish
        |> Actor.ref

    /// A distributed resource that aggregates an array of results.
    [<AutoSerializable(true)>]
    type ActorResultAggregator<'T> internal (capacity : int, source : ActorRef<ResultAggregatorMsg>) =

        interface ICloudResultAggregator<'T> with
            member __.Capacity = capacity
            member __.CurrentSize = source <!- GetCompleted
            member __.IsCompleted = source <!- IsCompleted
            member __.SetResult(index : int, value : 'T, overwrite : bool) = async {
                let pickle = Config.Serializer.Pickle value
                return! source <!- fun ch -> SetResult(index, pickle, overwrite, ch)
            }

            member __.ToArray () = async {
                let! pickles = source <!- ToArray
                return pickles |> Array.Parallel.map (fun p -> Config.Serializer.UnPickle<'T> p)
            }

            member __.Dispose () = async.Zero()

/// Defines a distributed result aggregator factory
type ActorResultAggregatorFactory(factory : ResourceFactory) =
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let! ref = factory.RequestResource(fun () -> ActorResultAggregator.create capacity)
            return new ActorResultAggregator<'T>(capacity, ref) :> _
        }