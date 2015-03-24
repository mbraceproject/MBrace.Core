using MBrace.CSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Flow.CSharp
{
    /// <summary>
    /// CSharp-friendly methods for CloudVector.
    /// </summary>
    public static class CloudVector
    {
        /// <summary>
        /// Update cache-map for given CloudVector.
        /// </summary>
        /// <typeparam name="TValue">Type of CloudVector.</typeparam>
        /// <param name="vector">Input CloudVector.</param>
        public static CloudAction Cache<TValue>(CloudVector<TValue> vector)
        {
            throw new System.NotImplementedException();
            //return new CloudAction(vector.Cache());
        }

        /// <summary>
        /// Reset cache-map for given CloudVector.
        /// </summary>
        /// <typeparam name="TValue">Type of CloudVector.</typeparam>
        /// <param name="vector">Input CloudVector.</param>
        public static CloudAction NoCache<TValue>(CloudVector<TValue> vector)
        {
            throw new System.NotImplementedException();
            //return new CloudAction(vector.NoCache());
        }

        /// <summary>
        /// Create a new CloudVector from given values.
        /// </summary>
        /// <typeparam name="TValue">Type of CloudVector.</typeparam>
        /// <param name="source">Input sequence.</param>
        /// <param name="maxPartitionSize">Max partitions size in bytes.</param>
        public static Cloud<CloudVector<TValue>>New<TValue>(IEnumerable<TValue> source, long maxPartitionSize)
        {
            return MBrace.Flow.CloudVector.New(source, maxPartitionSize, null);
        }

    }
}
