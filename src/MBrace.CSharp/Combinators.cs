using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MCloud = Nessos.MBrace.Cloud;

namespace Nessos.MBrace.CSharp
{
    public static partial class Cloud
    {
        public static Cloud<TResult []> Parallel<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            var wfs = workflows.Select(w => w.Computation).ToArray();
            return new Cloud<TResult[]>(MCloud.Parallel(wfs));
        }

        public static Cloud<TResult[]> Parallel<TResult>(params Cloud<TResult> [] workflows)
        {
            var wfs = workflows.Select(w => w.Computation).ToArray();
            return new Cloud<TResult[]>(MCloud.Parallel(wfs));
        }

        public static CloudUnit Log(string format, params object[] args)
        {
            return new CloudUnit(MCloud.Log(String.Format(format, args)));
        }

    }
}
