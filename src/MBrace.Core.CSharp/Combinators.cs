using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MCloud = Nessos.MBrace.Cloud;

namespace Nessos.MBrace.Core.CSharp
{
    public static partial class Cloud
    {
        public static Cloud<TResult []> Parallel<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            return new Cloud<TResult[]>(MCloud.Parallel(workflows.Select(w => w.Computation)));
        }

        public static Cloud<TResult[]> Parallel<TResult>(params Cloud<TResult> [] workflows)
        {
            return new Cloud<TResult[]>(MCloud.Parallel(workflows.Select(w => w.Computation)));
        }

        public static CloudUnit Log(string format, params object[] args)
        {
            return new CloudUnit(MCloud.Log(String.Format(format, args)));
        }

    }
}
