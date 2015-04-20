using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams;
using MBrace.Flow.Internals;
using System.IO;
using CFile = MBrace.CloudFile;
using MBrace.CSharp;

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
        public static CloudFlow<TSource> AsCloudFlow<TSource>(this TSource[] source)
        {
            return CloudFlowModule.ofArray<TSource>(source);
        }

        /// <summary>Constructs a CloudFlow from a PersistedCloudFlow.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TSource> AsCloudFlow<TSource>(this PersistedCloudFlow<TSource> source)
        {
            return CloudFlowModule.ofCloudVector<TSource>(source);
        }

        /// <summary>Constructs a CloudFlow from a collection of CloudFiles using the given reader.</summary>
        /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
        /// <param name="sources">The collection of CloudFiles.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TResult> AsCloudFlow<TResult>(this IEnumerable<CFile> sources, Func<System.IO.Stream, Task<TResult>> reader)
        {
            return CSharpProxy.OfCloudFiles<TResult>(sources, reader);
        }

        /// <summary>Transforms each element of the input CloudFlow.</summary>
        /// <param name="f">A function to transform items from the input CloudFlow.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TResult> Select<TSource, TResult>(this CloudFlow<TSource> stream, Func<TSource, TResult> f)
        {
            return CSharpProxy.Select(stream, f);
        }

        /// <summary>Filters the elements of the input CloudFlow.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TSource> Where<TSource>(this CloudFlow<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Where(stream, predicate);
        }


        /// <summary>Transforms each element of the input CloudFlow to a new stream and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the input CloudFlow.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The result CloudFlow.</returns>
        public static CloudFlow<TResult> SelectMany<TSource, TResult>(this CloudFlow<TSource> stream, Func<TSource, IEnumerable<TResult>> f)
        {
            return CSharpProxy.SelectMany(stream, f);
        }

        /// <summary>Returns a cloud stream with a new degree of parallelism.</summary>
        /// <param name="stream">The input cloud stream.</param>
        /// <param name="degreeOfParallelism">The degree of parallelism.</param>
        /// <returns>The result cloud stream.</returns>
        public static CloudFlow<TSource> WithDegreeOfParallelism<TSource>(this CloudFlow<TSource> stream, int degreeOfParallelism)
        {
            return CloudFlowModule.withDegreeOfParallelism(degreeOfParallelism, stream);
        }

        /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The final result.</returns>
        public static Cloud<TAccumulate> Aggregate<TSource, TAccumulate>(this CloudFlow<TSource> stream, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.Aggregate(stream, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
        /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
        /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The final result.</returns>
        public static CloudFlow<Tuple<TKey, TAccumulate>> AggregateBy<TSource, TKey, TAccumulate>(this CloudFlow<TSource> stream, Func<TSource, TKey> projection, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.AggregateBy(stream, projection, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
        /// <param name="stream">The input CloudFlow.</param>
        /// <param name="takeCount">The number of elements to return.</param>
        /// <returns>The result CloudFlow.</returns>   
        public static CloudFlow<TSource> OrderBy<TSource, TKey>(this CloudFlow<TSource> stream, Func<TSource, TKey> projection, int takeCount) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection, takeCount);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<int> Sum(this CloudFlow<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<long> Sum(this CloudFlow<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<float> Sum(this CloudFlow<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<double> Sum(this CloudFlow<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<decimal> Sum(this CloudFlow<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the total number of elements of the CloudFlow.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The total number of elements.</returns>
        public static Cloud<long> Count<TSource>(this CloudFlow<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }

        /// <summary>
        /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
        /// </summary>
        /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
        /// <param name="stream">The input CloudFlow.</param>
        public static CloudFlow<Tuple<TKey, long>> CountBy<TSource, TKey>(this CloudFlow<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.CountBy(stream, projection);
        }

        /// <summary>Creates an array from the given CloudFlow.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The result array.</returns>    
        public static Cloud<TSource[]> ToArray<TSource>(this CloudFlow<TSource> stream)
        {
            return CloudFlowModule.toArray(stream);
        }

        /// <summary>Creates a PersistedCloudFlow from the given CloudFlow.</summary>
        /// <param name="stream">The input CloudFlow.</param>
        /// <returns>The result PersistedCloudFlow.</returns>    
        public static Cloud<PersistedCloudFlow<TSource>> ToCloudVector<TSource>(this CloudFlow<TSource> stream)
        {
            return CloudFlowModule.toCloudVector(stream);
        }

    }
}
