using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp
{
    public class Cloud<TResult>
    {
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
