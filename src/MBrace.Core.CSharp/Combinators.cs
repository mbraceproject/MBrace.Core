using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.MBrace.Core.CSharp
{
    public static partial class Cloud
    {
        public static Cloud<TResult []> Parallel<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            throw new NotImplementedException();
        }
    }
}
