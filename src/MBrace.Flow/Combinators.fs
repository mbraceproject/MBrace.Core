namespace MBrace.Flow

open System
open System.Threading
open System.Text
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Workflows

open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

/// Provides CloudFlow producers.
type CloudFlow =

    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    static member OfArray (source : 'T []) : CloudFlow<'T> = Array.ToCloudFlow source

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    /// <param name="useCache">Make use of caching, if the collection supports it. Defaults to false.</param>
    /// <param name="sizeThresholdPerWorker">Restricts concurrent processing of collection partitions up to specified size per worker.</param>
    static member OfCloudCollection (collection : ICloudCollection<'T>, ?useCache:bool, ?sizeThresholdPerWorker:unit -> int64) : CloudFlow<'T> =
        CloudCollection.ToCloudFlow(collection, ?useCache = useCache, ?sizeThresholdPerWorker = sizeThresholdPerWorker)

    /// <summary>
    ///     Creates a CloudFlow instance from a finite collection of serializable enumerations.
    /// </summary>
    /// <param name="enumerations">Input enumerations.</param>
    static member OfSeqs (enumerations : seq<#seq<'T>>) : CloudFlow<'T> =
        Sequences.OfSeqs enumerations

    /// <summary>
    ///     Constructs a CloudFlow from a collection of CloudFiles using the given deserializer.
    /// </summary>
    /// <param name="paths">Cloud file input paths.</param>
    /// <param name="deserializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudFiles (paths : seq<string>, ?deserializer : System.IO.Stream -> seq<'T>, ?enableCache : bool, ?sizeThresholdPerCore : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let sizeThresholdPerCore = defaultArg sizeThresholdPerCore (1024L * 1024L * 256L)
                    let toCloudSeq (path : string) = CloudSequence.OfCloudFile(path, ?deserializer = deserializer, ?enableCache = enableCache)
                    let! cseqs = Sequential.map toCloudSeq paths
                    let collection = new PersistedCloudFlow<'T>(cseqs)
                    let threshold () = int64 Environment.ProcessorCount * sizeThresholdPerCore
                    let collectionFlow = CloudFlow.OfCloudCollection(collection, ?useCache = enableCache, sizeThresholdPerWorker = threshold)
                    return! collectionFlow.Apply collectorf projection combiner
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow from a collection of CloudFiles using the given serializer implementation.
    /// </summary>
    /// <param name="paths">Cloud file input paths.</param>
    /// <param name="serializer">Element deserialization function for cloud files.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudFiles (paths : seq<string>, serializer : ISerializer, ?enableCache : bool, ?sizeThresholdPerCore : int64) =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let deserializer (stream : System.IO.Stream) = serializer.SeqDeserialize(stream, leaveOpen = false)
                    let filesFlow = CloudFlow.OfCloudFiles(paths, deserializer, ?enableCache = enableCache, ?sizeThresholdPerCore = sizeThresholdPerCore)
                    return! filesFlow.Apply collectorf projection combiner
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow from a collection of text files using the given reader.
    /// </summary>
    /// <param name="paths">Cloud file input paths.</param>
    /// <param name="deserializer">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudFiles (paths : seq<string>, deserializer : System.IO.TextReader -> seq<'T>, ?encoding : Encoding, ?enableCache : bool, ?sizeThresholdPerCore : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let deserializer (stream : System.IO.Stream) =
                        let sr =
                            match encoding with
                            | None -> new System.IO.StreamReader(stream)
                            | Some e -> new System.IO.StreamReader(stream, e)

                        deserializer sr

                    let filesFlow = CloudFlow.OfCloudFiles(paths, deserializer, ?enableCache = enableCache, ?sizeThresholdPerCore = sizeThresholdPerCore)
                    return! filesFlow.Apply collectorf projection combiner
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow of lines from a collection of text files.
    /// </summary>
    /// <param name="paths">Paths to input cloud files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfCloudFilesByLine (paths : seq<string>, ?encoding : Encoding, ?sizeThresholdPerCore : int64) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let flow = CloudFlow.OfCloudFiles (paths, (fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding)), ?sizeThresholdPerCore = sizeThresholdPerCore)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a CloudFlow of all files in provided cloud directory using the given deserializer.
    /// </summary>
    /// <param name="dirPath">Input CloudDirectory.</param>
    /// <param name="deserializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudDirectory (dirPath : string, ?deserializer : System.IO.Stream -> seq<'T>, ?enableCache : bool, ?sizeThresholdPerCore : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! files = CloudFile.Enumerate dirPath
                let paths = files |> Array.map (fun f -> f.Path)
                let flow = CloudFlow.OfCloudFiles(paths, ?deserializer = deserializer, ?enableCache = enableCache, ?sizeThresholdPerCore = sizeThresholdPerCore)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///      Constructs a CloudFlow of all files in provided cloud directory using the given serializer implementation.
    /// </summary>
    /// <param name="dirPath">Input CloudDirectory.</param>
    /// <param name="deserializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudDirectory (dirPath : string, serializer : ISerializer, ?enableCache : bool, ?sizeThresholdPerCore : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! files = CloudFile.Enumerate dirPath
                let paths = files |> Array.map (fun f -> f.Path)
                let flow = CloudFlow.OfCloudFiles(paths, serializer = serializer, ?enableCache = enableCache, ?sizeThresholdPerCore = sizeThresholdPerCore)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a CloudFlow from all files in provided directory using the given reader.
    /// </summary>
    /// <param name="dirPath">Cloud file input paths.</param>
    /// <param name="deserializer">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudDirectory (dirPath : string, deserializer : System.IO.TextReader -> seq<'T>, ?encoding : Encoding, ?enableCache : bool, ?sizeThresholdPerCore : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! files = CloudFile.Enumerate dirPath
                let paths = files |> Array.map (fun f -> f.Path)
                let flow = CloudFlow.OfCloudFiles(paths, deserializer = deserializer, ?enableCache = enableCache, ?encoding = encoding, ?sizeThresholdPerCore = sizeThresholdPerCore)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a text CloudFlow by line from all files in supplied CloudDirectory.
    /// </summary>
    /// <param name="dirPath">Paths to input cloud files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfCloudDirectoryByLine (dirPath : string, ?encoding : Encoding, ?sizeThresholdPerCore : int64) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! files = CloudFile.Enumerate dirPath
                let paths = files |> Array.map (fun f -> f.Path)
                let flow = CloudFlow.OfCloudFilesByLine(paths, ?encoding = encoding, ?sizeThresholdPerCore = sizeThresholdPerCore)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a CloudFlow of lines from a single large text file.
    /// </summary>
    /// <param name="path">The path to the text file.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfCloudFileByLine (path : string, ?encoding : Encoding) : CloudFlow<string> = 
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! cseq = CloudSequence.FromLineSeparatedTextFile(path, ?encoding = encoding, enableCache = false, force = false)
                let collectionStream = CloudFlow.OfCloudCollection cseq
                return! collectionStream.Apply collectorf projection combiner
            }  
        }
       
    /// <summary>Creates a CloudFlow from the ReceivePort of a CloudChannel</summary>
    /// <param name="channel">the ReceivePort of a CloudChannel.</param>
    /// <param name="degreeOfParallelism">The number of concurrently receiving tasks</param>
    /// <returns>The result CloudFlow.</returns>
    static member OfCloudChannel (channel : IReceivePort<'T>, degreeOfParallelism : int) : CloudFlow<'T> =
        CloudChannel.ToCloudFlow(channel, degreeOfParallelism)


[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudFlows.
module CloudFlow =

    // TODO : move *Gen implementations to a consumer folder in project.
    //        haven't done this yet since it complicates inlining.

    //#region Intermediate functions

    let inline private run ctx a = Cloud.RunSynchronously(a, ctx.Resources,ctx.CancellationToken)

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline private mapGen  (f : ExecutionContext -> 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline map  (f : 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun _ x -> f x) flow

    /// <summary>Transforms each element of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline mapLocal (f : 'T -> Local<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun ctx x -> f x |> run ctx) flow

    let inline private collectGen (f : ExecutionContext -> 'T -> #seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collect (f : 'T -> #seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun _ x -> f x) flow 

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collectLocal (f : 'T -> Local<#seq<'R>>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun ctx x -> f x |> run ctx) flow 

    let inline private filterGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Filters the elements of the input CloudFlow.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filter (predicate : 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun _ x -> predicate x) flow 

    /// <summary>Filters the elements of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filterLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun ctx x -> predicate x |> run ctx) flow 

    /// <summary>Returns a cloud flow with a new degree of parallelism.</summary>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The result cloud flow.</returns>
    let inline withDegreeOfParallelism (degreeOfParallelism : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else
            { new CloudFlow<'T> with
                    member self.DegreeOfParallelism = Some degreeOfParallelism
                    member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                        flow.Apply collectorf projection combiner }

    // terminal functions

    let inline private foldGen (folder : ExecutionContext -> 'State -> 'T -> 'State) (combiner : ExecutionContext -> 'State -> 'State -> 'State) 
                               (state : ExecutionContext -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {  
            let results = new List<'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                member self.Iterator() = 
                    let accRef = ref <| state ctx 
                    results.Add(accRef)
                    {   Index = ref -1;
                        Func = (fun value -> accRef := folder ctx !accRef value);
                        Cts = cts }
                member self.Result = 
                    let mutable acc = state ctx
                    for result in results do
                            acc <- combiner ctx acc !result
                    acc }
        }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return! 
                flow.Apply 
                   (collectorf cts)
                   (fun x -> local { return x }) 
                   (fun values -> local { 
                       let! ctx = Cloud.GetExecutionContext()
                       let combined =
                            let mutable state = state ctx
                            for v in values do state <- combiner ctx state v
                            state
                       return combined })
        }



    /// <summary>Applies a locally executing cloud function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline foldLocal (folder : 'State -> 'T -> Local<'State>) (combiner : 'State -> 'State -> Local<'State>) 
                         (state : unit -> Local<'State>) (flow : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun ctx x y -> run ctx (folder x y)) (fun ctx x y -> run ctx (combiner x y)) (fun ctx -> run ctx (state ())) flow

    /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun _ x y -> folder x y) (fun _ x y -> combiner x y) (fun _ -> state ()) flow

    let inline private foldByGen (projection : ExecutionContext -> 'T -> 'Key) 
                                 (folder : ExecutionContext -> 'State -> 'T -> 'State) 
                                 (combiner : ExecutionContext -> 'State -> 'State -> 'State) 
                                 (state : ExecutionContext -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        let collectorf (cloudCts : ICloudCancellationTokenSource) (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                member self.Iterator() = 
                    {   Index = ref -1; 
                        Func =
                            (fun value -> 
                                    let mutable grouping = Unchecked.defaultof<_>
                                    let key = projection ctx value
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder ctx !acc value) 
                                    else
                                        grouping <- ref <| state ctx
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder ctx !acc value) 
                                    ());
                        Cts = cts }
                member self.Result = 
                    let partitions = dict 
                                     |> Seq.groupBy (fun keyValue -> Math.Abs(keyValue.Key.GetHashCode()) % totalWorkers)
                                     |> Seq.map (fun (key, keyValues) -> (key, keyValues |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value))))
                    partitions }
        }
        // Phase 1
        let shuffling = 
            cloud {
                let combiner' (result : _ []) = local { return Array.concat result }
                let! totalWorkers = match flow.DegreeOfParallelism with Some n -> local { return n } | None -> Cloud.GetWorkerCount()
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.Apply (collectorf cts totalWorkers) 
                                                  (fun keyValues -> local {
                                                        let dict = new Dictionary<int, PersistedCloudFlow<'Key * 'State>>() 
                                                        for (key, value) in keyValues do
                                                            let! values = PersistedCloudFlow.New(value, cache = false)
                                                            dict.[key] <- values
                                                        let values = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value)) 
                                                        return Seq.toArray values }) combiner'
                
                let merged =
                    keyValueArray
                    |> Seq.groupBy fst
                    |> Seq.map (fun (i,kva) -> i, kva |> Seq.map snd |> PersistedCloudFlow.Concat)
                    |> Seq.toArray
                return merged
            }
        let reducerf (cloudCts : ICloudCancellationTokenSource) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return { new Collector<int * PersistedCloudFlow<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                member self.Iterator() = 
                    {   Index = ref -1; 
                        Func =
                            (fun (_, keyValues) ->
                                let keyValues = Cloud.RunSynchronously(keyValues.ToEnumerable(), ctx.Resources, ctx.CancellationToken)
                                   
                                for (key, value) in keyValues do 
                                    let mutable grouping = Unchecked.defaultof<_>
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner ctx !acc value) 
                                    else
                                        grouping <- ref <| state ctx
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner ctx !acc value) 
                                ());
                        Cts = cts }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }
        // Phase 2
        let reducer (flow : CloudFlow<int * PersistedCloudFlow<'Key * 'State>>) : Cloud<PersistedCloudFlow<'Key * 'State>> = 
            cloud {
                let combiner' (result : PersistedCloudFlow<_> []) = local { return PersistedCloudFlow.Concat result }
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.Apply (reducerf cts) (fun keyValues -> PersistedCloudFlow.New(keyValues, cache = false)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'Key * 'State, 'S>>) (projection : 'S -> Local<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (CloudFlow.OfArray result)
                    return! (result' :> CloudFlow<_>).Apply collectorf projection combiner
                }  }


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator. The folder, combiner and state are locally executing cloud functions.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let foldByLocal (projection : 'T -> Local<'Key>) 
                           (folder : 'State -> 'T -> Local<'State>) 
                           (combiner : 'State -> 'State -> Local<'State>) 
                           (state : unit -> Local<'State>) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun ctx x -> projection x |> run ctx) (fun ctx x y -> folder x y |> run ctx) (fun ctx s1 s2 -> combiner s1 s2 |> run ctx) (fun ctx -> state () |> run ctx) flow 

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun _ x -> projection x) (fun _ x y -> folder x y) (fun _ s1 s2 -> combiner s1 s2) (fun _ -> state ()) flow 

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    let countBy (projection : 'T -> 'Key) (flow : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun _ctx x -> projection x) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun _ctx -> 0L) flow

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    let countByLocal (projection : 'T -> Local<'Key>) (flow : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun ctx x -> projection x |> run ctx) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun _ -> 0L) flow

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iter (action: 'T -> unit) (flow : CloudFlow< 'T >) : Cloud< unit > =
        fold (fun () x -> action x) (fun () () -> ()) (fun () -> ()) flow

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iterLocal (action: 'T -> Local<unit>) (flow : CloudFlow< 'T >) : Cloud< unit > =
        foldLocal (fun () x -> action x) (fun () () -> local { return () }) (fun () -> local { return () }) flow

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (flow : CloudFlow< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) flow


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumBy projection (flow : CloudFlow< 'T >) : Cloud< ^S >
            when ^S : (static member ( + ) : ^S * ^S -> ^S) 
            and  ^S : (static member Zero : ^S) = 
        fold (fun s x -> s + projection x) (+) (fun () -> LanguagePrimitives.GenericZero) flow

    /// <summary>Applies a key-generating locally executing cloud function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumByLocal (projection : 'T -> Local< ^Key >) (flow : CloudFlow< 'T >) : Cloud< ^Key >
            when ^Key : (static member ( + ) : ^Key * ^Key -> ^Key) 
            and  ^Key : (static member Zero : ^Key) = 
        foldGen (fun ctx s x -> s + run ctx (projection x)) (fun _ctx x y -> x + y) (fun _ctx -> LanguagePrimitives.GenericZero) flow

    /// <summary>Returns the total number of elements of the CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (flow : CloudFlow<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) flow

    /// <summary>Creates an array from the given CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (flow : CloudFlow<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) flow 
            return arrayCollector.ToArray()
        }

    /// <summary>Creates a PersistedCloudFlow from the given CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result PersistedCloudFlow.</returns>    
    let persist (flow : CloudFlow<'T>) : Cloud<PersistedCloudFlow<'T>> = PersistedCloudFlow.Persist(flow, enableCache = false)

    /// <summary>Creates a PersistedCloudFlow from the given CloudFlow, with its partitions cached to local memory.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result PersistedCloudFlow.</returns>
    let persistCached (flow : CloudFlow<'T>) : Cloud<PersistedCloudFlow<'T>> = PersistedCloudFlow.Persist(flow, enableCache = true)

    let inline private sortByGen comparer (projection : ExecutionContext -> 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {  
            let results = new List<List<'T>>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return 
              { new Collector<'T, List<'Key[] * 'T []>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    {   Index = ref -1; 
                        Func = (fun value -> list.Add(value));
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
                            keys.[counter] <- projection ctx value
                            values.[counter] <- value
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(), 
                                     values.Take(takeCount).ToArray())) }
        }
        let sortByComp = 
            cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let! results = flow.Apply (collectorf cts) (fun x -> local { return x }) (fun result -> local { match result with [||] -> return List() | _ -> return Array.reduce (fun left right -> left.AddRange(right); left) result })
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
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    values.Take(takeCount).ToArray()
                return result
            }
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (CloudFlow.OfArray result).Apply collectorf projection combiner
                }  
        }

    let inline private descComparer (comparer: IComparer<'T>) = { new IComparer<'T> with member __.Compare(x,y) = -comparer.Compare(x,y) }

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered using the given comparer for the keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsing (projection : 'T -> 'Key) comparer (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered descending by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescending (projection : 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByLocal (projection : 'T -> Local<'Key>) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsingLocal (projection : 'T -> Local<'Key>) comparer (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by descending keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescendingLocal (projection : 'T -> Local<'Key>) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    let inline private tryFindGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) =
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> if predicate ctx value then resultRef := Some value; cloudCts.Cancel() else ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return! flow.Apply (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun _ctx x -> predicate x) flow 

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFindLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun ctx x -> predicate x |> run ctx) flow 

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    let inline find (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFind predicate flow 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    let inline findLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFindLocal predicate flow 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    let inline private tryPickGen (chooser : ExecutionContext -> 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        
        let collectorf (cloudCts : ICloudCancellationTokenSource) = 
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'R option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return 
                    { new Collector<'T, 'R option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> match chooser ctx value with Some value' -> resultRef := Some value'; cloudCts.Cancel() | None -> ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return! flow.Apply (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun _ctx x -> chooser x) flow 

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPickLocal (chooser : 'T -> Local<'R option>) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun ctx x -> chooser x |> run ctx) flow 

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPick chooser flow 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    let inline pickLocal (chooser : 'T -> Local<'R option>) (flow : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPickLocal chooser flow 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Tests if any element of the flow satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFind predicate flow 
            return 
                match result with
                | Some _ -> true
                | None -> false
        }

    /// <summary>Tests if any element of the flow satisfies the given locally executing cloud predicate.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline existsLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFindLocal predicate flow 
            return 
                match result with
                | Some _ -> true
                | None -> false
        }


    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = exists (fun x -> not <| predicate x) flow
            return not result
        }


    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forallLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = existsLocal (fun x -> local { let! v = predicate x in return not v }) flow
            return not result
        }

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="n">The maximum number of items to take.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The resulting CloudFlow.</returns>
    let inline take (n : int) (flow: CloudFlow<'T>) : CloudFlow<'T> =
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
                let! results = flow.Apply (collectorF cts) (local.Return) (fun results -> local { return Array.concat results })
                return results.Take(n).ToArray()
            }
        { new CloudFlow<'T> with
              member __.DegreeOfParallelism = flow.DegreeOfParallelism
              member __.Apply<'S, 'R>(collectorF: Local<Collector<'T, 'S>>) (projection: 'S -> Local<'R>) combiner =
                  cloud {
                      let! result = gather
                      return! (CloudFlow.OfArray result).Apply collectorF projection combiner
                  }
        }

    /// <summary>Sends the values of CloudFlow to the SendPort of a CloudChannel</summary>
    /// <param name="channel">the SendPort of a CloudChannel.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let toCloudChannel (channel : ISendPort<'T>) (flow : CloudFlow<'T>)  : Cloud<unit> =
        flow |> iterLocal (fun v -> CloudChannel.Send(channel, v))

    /// <summary>
    ///     Returs true if the flow is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input flow.</param>
    /// <returns>true if the input flow is empty, false otherwise</returns>
    let inline isEmpty (flow : CloudFlow<'T>) : Cloud<bool> =
        cloud { let! isNotEmpty = flow |> exists (fun _ -> true) in return not isNotEmpty }


    /// <summary>Locates the maximum element of the flow by given key.</summary>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <param name="source">The input flow.</param>
    /// <returns>The maximum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    let inline maxBy<'T, 'Key when 'Key : comparison> (projection: 'T -> 'Key) (flow: CloudFlow<'T>) : Cloud<'T> =
        cloud {
            let! result =
                foldGen (fun _ state x ->
                               let keyOfX = projection x
                               match state with
                               | None -> Some (x, keyOfX)
                               | Some (_, keyOfValue) when keyOfValue < keyOfX -> Some (x, keyOfX)
                               | _ -> state)
                        (fun _ left right ->
                             match left, right with
                             | Some (_, leftKey), Some (_, rightKey) -> if rightKey > leftKey then right else left
                             | None, _ -> right
                             | _, None -> left)
                        (fun _ -> None)
                        flow

            match result with
            | None -> return! Cloud.Raise (new System.ArgumentException("The input flow was empty.", "flow"))
            | Some (maxVal, _) -> return maxVal
        }

    /// <summary>Locates the minimum element of the flow by given key.</summary>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <param name="source">The input flow.</param>
    /// <returns>The minimum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    let inline minBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (flow : CloudFlow<'T>) : Cloud<'T> =
        cloud {
            let! result =
                foldGen (fun _ state x ->
                             let keyOfX = projection x
                             match state with
                             | None -> Some (x, keyOfX)
                             | Some (_, keyOfValue) when keyOfValue > keyOfX -> Some (x, keyOfX)
                             | _ -> state)
                        (fun _ left right ->
                             match left, right with
                             | Some (_, leftKey), Some (_, rightKey) -> if rightKey > leftKey then left else right
                             | None, _ -> right
                             | _, None -> left)
                        (fun _ -> None)
                        flow

            match result with
            | None -> return! Cloud.Raise (new System.ArgumentException("The input flow was empty.", "flow"))
            | Some (minVal, _) -> return minVal
        }

    /// <summary>
    ///    Reduces the elements of the input flow to a single value via the given reducer function.
    ///    The reducer function is first applied to the first two elements of the flow.
    ///    Then, the reducer is applied on the result of the first reduction and the third element.
    //     The process continues until all the elements of the flow have been reduced.
    /// </summary>
    /// <param name="reducer">The reducer function.</param>
    /// <param name="flow">The input flow.</param>
    /// <returns>The reduced value.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    let inline reduce (reducer : 'T -> 'T -> 'T) (flow : CloudFlow<'T>) : Cloud<'T> =
        cloud {
            let! result =
                foldGen (fun _ state x -> match state with Some y -> Some (reducer y x) | None -> Some x)
                        (fun _ left right ->
                         match left, right with
                         | Some y, Some x -> Some (reducer y x)
                         | None, Some x -> Some x
                         | Some y, None -> Some y
                         | None, None -> None)
                        (fun _ -> None)
                        flow

            match result with
            | None -> return! Cloud.Raise (new System.ArgumentException("The input flow was empty.", "flow"))
            | Some reducedVal -> return reducedVal
        }
