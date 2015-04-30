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
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Flow

module Take =

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="n">The maximum number of items to take.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The resulting CloudFlow.</returns>
    let take (n : int) (flow: CloudFlow<'T>) : CloudFlow<'T> =
        let collectorF (cloudCts : ICloudCancellationTokenSource) =
            local {
                let results = new List<List<'T>>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T []> with
                      member __.DegreeOfParallelism = flow.DegreeOfParallelism
                      member __.Iterator() =
                          let list = new List<'T>()
                          results.Add(list)
                          { Index = ref -1
                            Func = (fun value -> if list.Count < n then list.Add(value) else cloudCts.Cancel())
                            Cts = cts }
                      member __.Result =
                          (results |> Seq.concat).Take(n) |> Seq.toArray
                     }
            }
        let gather =
            cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let! results = flow.WithEvaluators (collectorF cts) (local.Return) (fun results -> local { return Array.concat results })
                return results.Take(n).ToArray()
            }
        { new CloudFlow<'T> with
              member __.DegreeOfParallelism = flow.DegreeOfParallelism
              member __.WithEvaluators<'S, 'R>(collectorF: Local<Collector<'T, 'S>>) (projection: 'S -> Local<'R>) combiner =
                  cloud {
                      let! result = gather
                      return! (Array.ToCloudFlow result).WithEvaluators collectorF projection combiner
                  }
        }