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
using MBrace.Flow;

namespace MBrace.Flow.CSharp
{
    /// <summary>
    /// CloudFlow operations
    /// </summary>
    public static class CloudFlow
    {
        /// <summary>Wraps array as a CloudFlow.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TSource> OfArray<TSource>(TSource[] source)
        {
            return MBrace.Flow.CloudFlow.OfArray(source);
        }

        /// <summary>Constructs a CloudFlow of lines from a collection of text files.</summary>
        /// <param name="paths">Paths to input cloud files.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<string> OfCloudFileByLine(IEnumerable<string> paths)
        {
            return MBrace.Flow.CloudFlow.OfCloudFileByLine(paths);
        }

        /// <summary>Constructs a CloudFlow of lines from a single large text file.</summary>
        /// <param name="path">The path to the text file.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<string> OfCloudFileByLine(string path)
        {
            return MBrace.Flow.CloudFlow.OfCloudFileByLine(path);
        }

        /// <summary>Constructs a CloudFlow of lines from a collection of HTTP text files.</summary>
        /// <param name="paths">Paths to input cloud files.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<string> OfHTTPFileByLine(IEnumerable<string> paths)
        {
            return MBrace.Flow.CloudFlow.OfHttpFileByLine(paths);
        }

        /// <summary>Constructs a CloudFlow of lines from a single large HTTP text file.</summary>
        /// <param name="path">The path to the text file.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<string> OfHTTPFileByLine(string path)
        {
            return MBrace.Flow.CloudFlow.OfHttpFileByLine(path);
        }

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
        public static CloudFlow<Tuple<TKey, IEnumerable<TSource>>> GroupBy<TSource, TKey>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection)
        {
            return CloudFlowModule.groupBy(FSharpFunc.Create(projection), flow);
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

        /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
        /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
        /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="flow">The input CloudFlow.</param>
        /// <returns>The final result.</returns>
        public static CloudFlow<Tuple<TKey, TAccumulate>> AggregateBy<TSource, TKey, TAccumulate>(this CloudFlow<TSource> flow, Func<TSource, TKey> projection, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CloudFlowModule.foldBy<TSource, TKey, TAccumulate>(FSharpFunc.Create(projection), FSharpFunc.Create(folder), FSharpFunc.Create(combiner), FSharpFunc.Create(state), flow);
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

    }
}
