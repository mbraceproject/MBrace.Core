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
    /// Cloud Unit.
    /// </summary>
    public class CloudUnit : Cloud<Unit>
    {
        internal CloudUnit(MBrace.Cloud<Unit> cloud) : base(cloud)
        {
            
        }
    }
}
