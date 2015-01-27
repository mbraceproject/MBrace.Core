using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp
{
    /// <summary>
    ///     Cloud workflow type.
    /// </summary>
    /// <typeparam name="TResult">Result returned by the computation.</typeparam>
    public class Cloud<TResult>
    {
        /// <summary>
        ///     Returns the contained F# workflow.
        /// </summary>
        public MBrace.Cloud<TResult> Computation { get; private set; }

        internal Cloud(MBrace.Cloud<TResult> cloud)
        {
            this.Computation = cloud;
        }
    }

    /// <summary>
    /// Encapsulates a Cloud workflow that does not return a value.
    /// </summary>
    public class CloudAction : Cloud<Unit>
    {
        internal CloudAction(MBrace.Cloud<Unit> cloud) : base(cloud)
        {
            
        }
    }
}
