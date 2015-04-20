using MBrace.CSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Flow.CSharp
{
    /// <summary>
    /// CSharp-friendly methods for PersistedCloudFlow.
    /// </summary>
    public static class PersistedCloudFlow
    {
        /// <summary>
        /// Update cache-map for given PersistedCloudFlow.
        /// </summary>
        /// <typeparam name="TValue">Type of PersistedCloudFlow.</typeparam>
        /// <param name="vector">Input PersistedCloudFlow.</param>
        public static CloudAction Cache<TValue>(PersistedCloudFlow<TValue> vector)
        {
            throw new System.NotImplementedException();
            //return new CloudAction(vector.Cache());
        }

        /// <summary>
        /// Reset cache-map for given PersistedCloudFlow.
        /// </summary>
        /// <typeparam name="TValue">Type of PersistedCloudFlow.</typeparam>
        /// <param name="vector">Input PersistedCloudFlow.</param>
        public static CloudAction NoCache<TValue>(PersistedCloudFlow<TValue> vector)
        {
            throw new System.NotImplementedException();
            //return new CloudAction(vector.NoCache());
        }

        /// <summary>
        /// Create a new PersistedCloudFlow from given values.
        /// </summary>
        /// <typeparam name="TValue">Type of PersistedCloudFlow.</typeparam>
        /// <param name="source">Input sequence.</param>
        /// <param name="maxPartitionSize">Max partitions size in bytes.</param>
        public static Cloud<PersistedCloudFlow<TValue>>New<TValue>(IEnumerable<TValue> source, long maxPartitionSize)
        {
            return MBrace.Flow.PersistedCloudFlow.New(source, maxPartitionSize, null);
        }

    }
}
