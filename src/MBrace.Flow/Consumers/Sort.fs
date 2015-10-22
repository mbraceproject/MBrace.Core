namespace MBrace.Flow.Internals.Consumers

open System
open System.IO
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Sort =

    /// <summary>
    ///     Generic comparer CloudFlow combinator
    /// </summary>
    /// <param name="comparer"></param>
    /// <param name="projection"></param>
    /// <param name="takeCount"></param>
    /// <param name="flow"></param>
    let sortBy (comparer : IComparer<'Key> option) (projection : 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        let mkCollector () = local {
            let results = new List<List<'T>>()
            let! ct = Cloud.CancellationToken
            let cts = CancellationTokenSource.CreateLinkedTokenSource(ct.LocalToken)
            return
              { new Collector<'T, List<'Key[] * 'T []>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    let list = new List<'T>()
                    results.Add(list)
                    {   Func = (fun value -> list.Add(value));
                        Cts = cts }
                member self.Result =
                    let count = results |> Seq.sumBy (fun list -> list.Count)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for list in results do
                        for i = 0 to list.Count - 1 do
                            let value = list.[i]
                            counter <- counter + 1
                            keys.[counter] <- projection value
                            values.[counter] <- value

                    match comparer with
                    | None -> Sort.parallelSort Environment.ProcessorCount keys values
                    | Some cmp -> Array.Sort(keys, values, cmp) // note that this performs sequential sort operation!!!!

                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(),
                                     values.Take(takeCount).ToArray())) }
        }

        let sortByComp =
            cloud {
                let! results = flow.WithEvaluators (mkCollector ()) (fun x -> local { return x }) (fun result -> local { match result with [||] -> return List() | _ -> return Array.reduce (fun left right -> left.AddRange(right); left) result })
                let result =
                    let count = results |> Seq.sumBy (fun (keys, _) -> keys.Length)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for (keys', values') in results do
                        for i = 0 to keys'.Length - 1 do
                            counter <- counter + 1
                            keys.[counter] <- keys'.[i]
                            values.[counter] <- values'.[i]

                    match comparer with
                    | None -> Sort.parallelSort Environment.ProcessorCount keys values
                    | Some cmp -> Array.Sort(keys, values, cmp) // note that this performs sequential sort operation!!!!

                    values.Take(takeCount).ToArray()
                return result
            }

        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'T, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner =
                cloud {
                    let! result = sortByComp
                    return! (Array.ToCloudFlow result).WithEvaluators collectorf projection combiner
                }
        }