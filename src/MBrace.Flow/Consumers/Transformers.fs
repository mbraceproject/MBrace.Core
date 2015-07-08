namespace MBrace.Flow.Internals.Consumers

open System.IO
open System.Runtime.Serialization
open System.Collections.Generic
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Transformers =

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let mapGen (f : ExecutionContext -> 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index;
                                Func = (fun value -> iter (f ctx value));
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Generic filter flow transformer.
    /// </summary>
    /// <param name="predicate"></param>
    /// <param name="flow"></param>
    let filterGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index;
                                Func = (fun value -> if predicate ctx value then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Generic choose flow transformer.
    /// </summary>
    /// <param name="chooser"></param>
    /// <param name="flow"></param>
    let chooseGen (chooser : ExecutionContext -> 'T -> 'R option) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index;
                                Func = (fun value ->
                                          match chooser ctx value with
                                          | Some value' -> iter value'
                                          | None -> ())
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Generic collect flow transformer.
    /// </summary>
    /// <param name="f">Collector function.</param>
    /// <param name="flow">Input cloud flow.</param>
    let collectGen (f : ExecutionContext -> 'T -> #seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index;
                                Func =
                                    (fun value ->
                                        let (Stream streamf) = Stream.ofSeq (f ctx value)
                                        let cts = CancellationTokenSource.CreateLinkedTokenSource(iterator.Cts.Token)
                                        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ()); Cont = iter; Cts = cts } in bulk ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                flow.WithEvaluators collectorf' projection combiner }