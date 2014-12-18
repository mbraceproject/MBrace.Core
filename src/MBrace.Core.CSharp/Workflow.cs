using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.MBrace.Core.CSharp
{
    /// <summary>
    /// Provides methods to create basic Cloud workflows.
    /// </summary>
    public static partial class Cloud
    {
        private static CloudBuilder builder = CloudBuilderModule.cloud;

        /// <summary>
        /// Constructs a cloud computation that returns the given value.
        /// </summary>
        /// <typeparam name="TResult">Type of the computation.</typeparam>
        /// <param name="value">Value to return.</param>
        public static Cloud<TResult> New<TResult>(TResult value)
        {
            return builder.Return(value);
        }

        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, Cloud<TResult>> continuation)
        {
            var fsFunc = continuation.AsFSharpFunc();
            return builder.Bind<TSource, TResult>(workflow, fsFunc);
        }

        private static void test ()
        {
            var wf = Cloud.New(42)
                        .Then(x => Cloud.New(x * x))
                        .Then(x => CloudAtom.New(x));
        }
    }
}
