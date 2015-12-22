using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using MBrace.Core.Internals;
using MBrace.Core.Internals.CSharpProxy;
using MBrace.Flow.Internals;
using System.IO;
using MBrace.Core;
using MBrace.Core.CSharp;
using MBrace.Flow;

namespace MBrace.Flow.CSharp
{
    /// <summary>
    /// CloudFlow operations
    /// </summary>
    public static class CloudFlow
    {
        #region "Producers"

        private const long sizeThresholdPerCore = 1024 * 1024 * 256;

        /// <summary>Wraps array as a CloudFlow.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TSource> OfArray<TSource>(TSource[] source)
        {
            return MBrace.Flow.CloudFlow.OfArray(source);
        }

        /// <summary>
        ///      Creates a CloudFlow according to partitions of provided cloud collection.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="collection">Input cloud collection.</param>
        /// <param name="sizeThresholdPerWorker">Restricts concurrent processing of collection partitions up to specified size per worker.</param>
        /// <returns>A CloudFlow that performs distributed computation based on Cloud collection partitioning scheme.</returns>
        public static CloudFlow<TSource> OfCloudCollection<TSource>(ICloudCollection<TSource> collection, Func<long> sizeThresholdPerWorker = null)
        {
            return MBrace.Flow.CloudFlow.OfCloudCollection(collection, sizeThresholdPerWorker: Option.FromNullable(sizeThresholdPerWorker).Select(FSharpFunc.Create));
        }

        /// <summary>
        ///     Creates a CloudFlow according to a collection of serializable enumerables.
        ///     Workload is partitioned based on the number of enumerables provided.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="enumerables">Input enumerable data source.</param>
        /// <returns>A CloudFlow that performs distributed computation on supplied enumerables.</returns>
        public static CloudFlow<TSource> OfEnumerables<TSource>(IEnumerable<IEnumerable<TSource>> enumerables)
        {
            return MBrace.Flow.CloudFlow.OfSeqs<IEnumerable<TSource>, TSource>(enumerables);
        }

        /// <summary>
        ///     Constructs a CloudFlow from a collection of CloudFiles using the given deserializer.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="paths">Cloud file input paths.</param>
        /// <param name="deserializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudFiles<TSource>(IEnumerable<string> paths, Func<System.IO.Stream, IEnumerable<TSource>> deserializer = null, 
                                                                        long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudFiles(paths, deserializer: Option.FromNullable(deserializer).Select(FSharpFunc.Create), 
                                                                sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow from a collection of CloudFiles using the given serializer implementation.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="paths">Cloud file input paths.</param>
        /// <param name="serializer">Element deserialization function for cloud files.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudFiles<TSource>(IEnumerable<string> paths, ISerializer serializer, long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudFiles<TSource>(paths, serializer: serializer, sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow from a collection of text files using the given reader.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="paths">Cloud file input paths.</param>
        /// <param name="deserializer">A function to transform the contents of a CloudFile to a stream of elements.</param>
        /// <param name="encoding">Text encoding used by the underlying text files.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudFiles<TSource>(IEnumerable<string> paths, Func<TextReader, IEnumerable<TSource>> deserializer, 
                                                                        Encoding encoding = null, long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudFiles<TSource>(paths, FSharpFunc.Create(deserializer), encoding: Option.FromNullable(encoding), 
                                                                    sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow of all files in provided cloud directory using the given deserializer.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="directoryPath">Input CloudDirectory.</param>
        /// <param name="deserializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudDirectory<TSource>(string directoryPath, Func<System.IO.Stream, IEnumerable<TSource>> deserializer = null, 
                                                                        long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudDirectory<TSource>(directoryPath, deserializer: Option.FromNullable(deserializer).Select(FSharpFunc.Create),
                                                                        sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow of all files in provided cloud directory using the given serializer implementation.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="directoryPath">Input CloudDirectory.</param>
        /// <param name="serializer">Element deserialization function for cloud files. Defaults to runtime serializer.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudDirectory<TSource>(string directoryPath, ISerializer serializer, long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudDirectory<TSource>(directoryPath, serializer, sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow from all files in provided directory using the given reader.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="directoryPath">Cloud file input paths.</param>
        /// <param name="deserializer">A function to transform the contents of a CloudFile to a stream of elements.</param>
        /// <param name="encoding">Text encoding used by the underlying text files.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed computation on deserialized cloud file contents.</returns>
        public static CloudFlow<TSource> OfCloudDirectory<TSource>(string directoryPath, Func<TextReader, IEnumerable<TSource>> deserializer,
                                                                        Encoding encoding = null, long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudDirectory<TSource>(directoryPath, FSharpFunc.Create(deserializer), encoding: Option.FromNullable(encoding),
                                                                    sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }



        /// <summary>
        ///     Constructs a CloudFlow of lines from a collection of text files.
        /// </summary>
        /// <param name="paths">Paths to input cloud files.</param>
        /// <param name="encoding">Optional encoding.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed processing on cloud text files by line.</returns>
        public static CloudFlow<string> OfCloudFileByLine(IEnumerable<string> paths, Encoding encoding = null, long sizeThresholdPerCore = sizeThresholdPerCore)
        {
            return MBrace.Flow.CloudFlow.OfCloudFileByLine(paths, encoding: encoding.ToOption(), sizeThresholdPerCore: sizeThresholdPerCore.ToOption());
        }

        /// <summary>
        ///     Constructs a CloudFlow of lines from a single large text file.
        /// </summary>
        /// <param name="path">The path to the text file.</param>
        /// <param name="encoding">Optional encoding.</param>
        /// <returns>A CloudFlow that performs distributed processing on cloud text file by line.</returns>
        public static CloudFlow<string> OfCloudFileByLine(string path, Encoding encoding = null)
        {
            return MBrace.Flow.CloudFlow.OfCloudFileByLine(path, encoding: Option.FromNullable(encoding));
        }

        /// <summary>
        ///     Constructs a text CloudFlow by line from all files in supplied CloudDirectory.
        /// </summary>
        /// <param name="directoryPath">Paths to input cloud files.</param>
        /// <param name="encoding">Optional encoding.</param>
        /// <param name="sizeThresholdPerCore">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
        /// <returns>A CloudFlow that performs distributed processing on cloud text files by line.</returns>
        public static CloudFlow<string> OfCloudDirectoryByLine(string directoryPath, Encoding encoding = null, long? sizeThresholdPerCore = null)
        {
            return MBrace.Flow.CloudFlow.OfCloudDirectoryByLine(directoryPath, encoding: Option.FromNullable(encoding), sizeThresholdPerCore: Option.FromNullable(sizeThresholdPerCore));
        }

        /// <summary>
        ///     Constructs a CloudFlow of lines from a single HTTP text files.
        /// </summary>
        /// <param name="paths">Url path to the text files.</param>
        /// <param name="encoding">Optional encoding.</param>
        /// <returns>A CloudFlow that performs a distributed computation on given collection of http text files.</returns>
        public static CloudFlow<string> OfHttpFileByLine(IEnumerable<string> paths, Encoding encoding = null)
        {
            return MBrace.Flow.CloudFlow.OfHttpFileByLine(paths, encoding: Option.FromNullable(encoding));
        }

        /// <summary>
        ///     Constructs a CloudFlow of lines from a single HTTP text file.
        /// </summary>
        /// <param name="path">Url path to the text file.</param>
        /// <param name="encoding">Optional encoding.</param>
        /// <returns>A CloudFlow that performs a distributed computation on given collection of http text files.</returns>
        public static CloudFlow<string> OfHttpFileByLine(string path, Encoding encoding = null)
        {
            return MBrace.Flow.CloudFlow.OfHttpFileByLine(path, encoding: Option.FromNullable(encoding));
        }

        /// <summary>
        ///     Creates a CloudFlow from the ReceivePort of a CloudQueue.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="queue">the ReceivePort of a CloudQueue.</param>
        /// <param name="degreeOfParallelism">The number of concurrently receiving tasks.</param>
        /// <returns>A CloudFlow that distributively performs operations on received messages from queue.</returns>
        public static CloudFlow<TSource> OfCloudQueue<TSource>(CloudQueue<TSource> queue, int degreeOfParallelism)
        {
            return MBrace.Flow.CloudFlow.OfCloudQueue<TSource>(queue, degreeOfParallelism);
        }

        #endregion

        #region "Transformers"

        /// <summary>Transforms each element of the input CloudFlow.</summary>
        /// <param name="f">A function to transform items from the input CloudFlow.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TResult> Select<TSource, TResult>(this CloudFlow<TSource> flow, Func<TSource, TResult> f)
        {
            return CloudFlowModule.map(FSharpFunc.Create(f), flow);
        }

        /// <summary>Filters the elements of the input CloudFlow.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TSource> Where<TSource>(this CloudFlow<TSource> flow, Func<TSource, bool> predicate)
        {
            return CloudFlowModule.filter(FSharpFunc.Create(predicate), flow);
        }

        /// <summary>Transforms each element of the input CloudFlow to a new flow and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the input CloudFlow.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TResult> SelectMany<TSource, TResult>(this CloudFlow<TSource> flow, Func<TSource, IEnumerable<TResult>> f)
        {
            return CloudFlowModule.collect<TSource, IEnumerable<TResult>, TResult>(FSharpFunc.Create(f), flow);
        }

        /// <summary>Returns a cloud flow with a new degree of parallelism.</summary>
        /// <param name="flow">The input cloud flow.</param>
        /// <param name="degreeOfParallelism">The degree of parallelism.</param>
        /// <returns>The result cloud flow.</returns>
        public static CloudFlow<TSource> WithDegreeOfParallelism<TSource>(this CloudFlow<TSource> flow, int degreeOfParallelism)
        {
            return CloudFlowModule.withDegreeOfParallelism(degreeOfParallelism, flow);
        }

        #endregion

        #region "Consumers"

        /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
        /// <param name="n">The maximum number of items to take.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The resulting CloudFlow.</returns>
        public static CloudFlow<TSource> Take<TSource>(this CloudFlow<TSource> flow, int n)
        {
            return CloudFlowModule.take(n, flow);
        }

        /// <summary>
        ///     Applies a key-generating function to each element of the input flow and yields a flow of unique keys and a sequence of all elements that have each key.
        /// <remarks>
        ///     Note: This combinator may be very expensive; for example if the group sizes are expected to be large.
        ///     If you intend to perform an aggregate operation, such as sum or average,
        ///     you are advised to use CloudFlow.foldBy or CloudFlow.countBy, for much better performance.
        /// </remarks>
        /// </summary>
        /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
        /// <param name="flow">The input flow.</param>
        /// <returns>A flow of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>
        public static CloudFlow<KeyValuePair<TKey, IEnumerable<TSource>>> GroupBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection)
        {
            return CloudFlowModule
                    .groupBy(FSharpFunc.Create(projection), flow)
                    .Select(tuple => new KeyValuePair<TKey, IEnumerable<TSource>>(tuple.Item1, tuple.Item2));
        }

        /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The final result.</returns>
        public static Cloud<TAccumulate> Aggregate<TSource, TAccumulate>(this CloudFlow<TSource> flow, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CloudFlowModule.fold<TAccumulate, TSource>(FSharpFunc.Create(folder), FSharpFunc.Create(combiner), FSharpFunc.Create(state), flow);
        }

        /// <summary>
        ///     Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.
        /// </summary>
        /// <typeparam name="TSource">Source flow element type.</typeparam>
        /// <typeparam name="TKey">Element key type.</typeparam>
        /// <typeparam name="TState">Accumulated state type.</typeparam>
        /// <param name="flow">Source CloudFlow.</param>
        /// <param name="projection">Key projection function.</param>
        /// <param name="folder">State updater function on the initial inputs.</param>
        /// <param name="combiner">State combiner function.</param>
        /// <param name="initializer">State initializer function.</param>
        /// <returns>A CloudFlow that has grouped accumulated states by key.</returns>
        public static CloudFlow<KeyValuePair<TKey, TState>> AggregateBy<TSource, TKey, TState>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection,
                                                                                                        Func<TState> initializer,
                                                                                                        Func<TState, TSource, TState> folder,
                                                                                                        Func<TState, TState, TState> combiner)
        {
            return CloudFlowModule
                    .foldBy(projection.ToFSharpFunc(), folder.ToFSharpFunc(), combiner.ToFSharpFunc(), initializer.ToFSharpFunc(), flow)
                    .Select(tuple => new KeyValuePair<TKey, TState>(tuple.Item1, tuple.Item2));
        }

        /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <param name="takeCount">The number of elements to return.</param>
        /// <returns>The result CloudFlow.</returns>   
        public static CloudFlow<TSource> OrderBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection, int takeCount) where TKey : IComparable<TKey>
        {
            return CloudFlowModule.sortBy(FSharpFunc.Create(projection), takeCount, flow);
        }

        /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <param name="takeCount">The number of elements to return.</param>
        /// <returns>The result CloudFlow.</returns>   
        public static CloudFlow<TSource> OrderByDescending<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection, int takeCount) where TKey : IComparable<TKey>
        {
            return CloudFlowModule.sortByDescending(FSharpFunc.Create(projection), takeCount, flow);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<int> Sum(this CloudFlow<int> flow)
        {
            return CloudFlowModule.sum(flow);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<long> Sum(this CloudFlow<long> flow)
        {
            return CloudFlowModule.sum(flow);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<float> Sum(this CloudFlow<float> flow)
        {
            return CloudFlowModule.sum(flow);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<double> Sum(this CloudFlow<double> flow)
        {
            return CloudFlowModule.sum(flow);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<decimal> Sum(this CloudFlow<decimal> flow)
        {
            return CloudFlowModule.sum(flow);
        }

        /// <summary>Returns the total number of elements of the CloudFlow.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The total number of elements.</returns>
        public static Cloud<long> Count<TSource>(this CloudFlow<TSource> flow)
        {
            return CloudFlowModule.length(flow);
        }

        /// <summary>
        /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
        /// </summary>
        /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
        /// <param name="flow">The input CloudFlow.</param>
        public static CloudFlow<Tuple<TKey, long>> CountBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection)
        {
            return CloudFlowModule.countBy(FSharpFunc.Create(projection), flow);
        }

        /// <summary>
        /// Locates the maximum element of the flow by given key.
        /// </summary>
        /// <param name="projection">A function that maps items from the input CloudFlow to comparable keys.</param>
        /// <param name="flow">The input CloudFlow.</param>
        public static Cloud<TSource> MaxBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection)
        {
            return CloudFlowModule.maxBy(FSharpFunc.Create(projection), flow);
        }

        /// <summary>
        /// Locates the minimum element of the flow by given key.
        /// </summary>
        /// <param name="projection">A function that maps items from the input CloudFlow to comparable keys.</param>
        /// <param name="flow">The input CloudFlow.</param>
        public static Cloud<TSource> MinBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection)
        {
            return CloudFlowModule.minBy(FSharpFunc.Create(projection), flow);
        }

        /// <summary>
        ///     Returns a flow that contains no duplicate elements according to their generic hash and equality comparisons. 
        ///     If an element occurs multiple times in the flow then only one is retained.
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <param name="flow">Input cloud flow.</param>
        /// <returns>A CloudFlow with unique occurences of elements.</returns>
        public static CloudFlow<TSource> Distinct<TSource>(this CloudFlow<TSource> flow) where TSource : IComparable
        {
            return CloudFlowModule.distinct(flow);
        }

        /// <summary>
        ///     Returns a flow that contains no duplicate entries according to the generic hash and equality comparisons on the keys returned by the given key-generating function. 
        ///     If an element occurs multiple times in the flow then only one is retained.
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <typeparam name="TKey">Distinction key type.</typeparam>
        /// <param name="flow">Input cloud flow.</param>
        /// <param name="projection">Key projection function.</param>
        /// <returns>A CloudFlow with unique occurences of elements up to key comparison.</returns>
        public static CloudFlow<TSource> DistinctBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection) where TKey : IComparable
        {
            return CloudFlowModule.distinctBy(projection.ToFSharpFunc(), flow);
        }

        /// <summary>
        ///     Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <param name="flow">Input cloud flow.</param>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <returns>The first element that satisfies the predicate.</returns>
        public static Cloud<TSource> Find<TSource>(this CloudFlow<TSource> flow, Func<TSource, bool> predicate)
        {
            return CloudFlowModule.find(predicate.ToFSharpFunc(), flow);
        }

        /// <summary>
        ///     Returns true if there exists an element in the CloudFlow which satisfies the predicate.
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <param name="flow">Input cloud flow.</param>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <returns>Returns true if there exists an element in the CloudFlow which satisfies the predicate.</returns>
        public static Cloud<bool> Any<TSource>(this CloudFlow<TSource> flow, Func<TSource, bool> predicate)
        {
            return CloudFlowModule.exists(predicate.ToFSharpFunc(), flow);
        }

        /// <summary>
        ///     Returns true if all elements in the CloudFlow satisfy the supplied predicate.
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <param name="flow">Input cloud flow.</param>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <returns>Returns true if all elements in the CloudFlow satisfy the supplied predicate.</returns>
        public static Cloud<bool> All<TSource>(this CloudFlow<TSource> flow, Func<TSource, bool> predicate)
        {
            return CloudFlowModule.forall(predicate.ToFSharpFunc(), flow);
        }

        /// <summary>Creates an array from the given CloudFlow.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The result array.</returns>    
        public static Cloud<TSource[]> ToArray<TSource>(this CloudFlow<TSource> flow)
        {
            return CloudFlowModule.toArray(flow);
        }

        /// <summary>Creates a PersistedCloudFlow from the given CloudFlow.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <param name="storageLevel">Desired storage level for the persisted CloudFlow.</param>
        /// <returns>The result PersistedCloudFlow.</returns>    
        public static Cloud<PersistedCloudFlow<TSource>> Persist<TSource>(this CloudFlow<TSource> flow, StorageLevel storageLevel)
        {
            return CloudFlowModule.persist(storageLevel, flow);
        }

        /// <summary>Creates a PersistedCloudFlow from the given CloudFlow.</summary>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The result PersistedCloudFlow.</returns>    
        public static Cloud<PersistedCloudFlow<TSource>> Cache<TSource>(this CloudFlow<TSource> flow)
        {
            return CloudFlowModule.cache(flow);
        }

        /// <summary>
        ///     Enqueues all values in the cloud flow to the supplied cloud queue
        /// </summary>
        /// <typeparam name="TSource">Cloud flow element type.</typeparam>
        /// <param name="flow">The input CloudFlow.</param>
        /// <param name="queue">Target CloudQueue to enqueue messages.</param>
        /// <returns>A Cloud computation which distributively enqueues all elements in the cloud flow to the supplied queue.</returns>
        public static Cloud<Unit> ToCloudQueue<TSource>(this CloudFlow<TSource> flow, CloudQueue<TSource> queue)
        {
            return CloudFlowModule.toCloudQueue(queue, flow);
        }

        #endregion

    }
}
