using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MCloud = MBrace.Cloud;

namespace MBrace.CSharp
{
    public static partial class Cloud
    {
        /// <summary>
        ///     Writes the following message to MBrace logging interface.
        /// </summary>
        /// <param name="format">Format string.</param>
        /// <param name="args">Arguments to format string.</param>
        public static CloudAction Log(string format, params object[] args)
        {
            return new CloudAction(MCloud.Log(String.Format(format, args)));
        }

        /// <summary>
        ///     Asynchronously suspends workflow for given amount of milliseconds.
        /// </summary>
        /// <param name="millisecondsDue">Milliseconds to suspend computation.</param>
        public static CloudAction Sleep(int millisecondsDue)
        {
            return new CloudAction(MCloud.Sleep(millisecondsDue));
        }

        /// <summary>
        /// Gets total number of available workers in cluster context.
        /// </summary>
        /// <returns>The number of workers.</returns>
        public static Cloud<int> GetWorkerCount()
        {
            return MCloud.GetWorkerCount();
        }
    }
}
