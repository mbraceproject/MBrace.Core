using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.MBrace.Core.CSharp
{
    public class Cloud<TResult>
    {
        public MBrace.Cloud<TResult> Computation { get; private set; }

        internal Cloud(Nessos.MBrace.Cloud<TResult> cloud)
        {
            this.Computation = cloud;
        }
    }

    /// <summary>
    /// Cloud Unit.
    /// </summary>
    public partial class CloudUnit : Cloud<Unit>
    {
        internal CloudUnit(Nessos.MBrace.Cloud<Unit> cloud) : base(cloud)
        {
            
        }
    }
}
