namespace MBrace.Flow

open Nessos.Streams

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

type internal Array =

    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    static member ToCloudFlow (source : 'T [], ?degreeOfParallelism : int) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = degreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'T, 'S>>) (projection : 'S -> LocalCloud<'R>) (combiner : 'R [] -> LocalCloud<'R>) =
                cloud {
                    // local worker ParStream workflow
                    let createTask array = local {
                        let! collector = collectorf
                        let parStream = ParStream.ofArray array 
                        let collectorResult = parStream.Apply (collector.ToParStreamCollector())
                        return! projection collectorResult
                    }

                    let! collector = collectorf
                    let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                    let! workers = Cloud.GetAvailableWorkers() 
                    // force deterministic scheduling sorting by worker id
                    let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                    let workers =
                        match collector.DegreeOfParallelism with
                        | None -> workers
                        | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]
                    
                    let! results =
                        if targetedworkerSupport then
                            source
                            |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                            |> Seq.filter (not << Array.isEmpty << snd)
                            |> Seq.map (fun (w,partition) -> createTask partition, w)
                            |> Cloud.Parallel
                        else
                            source
                            |> WorkerRef.partition workers
                            |> Seq.filter (not << Array.isEmpty << snd)
                            |> Seq.map (createTask << snd)
                            |> Cloud.Parallel

                    return! combiner results 
                } }