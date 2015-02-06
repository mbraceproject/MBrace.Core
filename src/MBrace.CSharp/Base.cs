using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp
{
    /// <summary>
    /// Encapsulates a Cloud workflow that does not return a value.
    /// </summary>
    [Serializable]
    public class CloudAction 
    {
        /// <summary>
        /// For internal use only. Gets encapsulated workflow.
        /// </summary>
        public Cloud<Unit> Body { get; private set; }

        internal CloudAction(MBrace.Cloud<Unit> body) 
        {
            this.Body = body;
        }
    }
}
