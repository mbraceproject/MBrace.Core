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
        public static Cloud<TResult> Return<TResult>(TResult value)
        {
            return builder.Return(value);
        }

        public static Cloud<TResult> New<TResult>(Func<Cloud<TResult>> delay)
        {
            return builder.Delay()
        }

        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, Cloud<TResult>> continuation)
        {
            var fsFunc = continuation.AsFSharpFunc();
            return builder.Bind<TSource, TResult>(workflow, fsFunc);
        }

        // Linq comprehension syntax friendly methods.

        public static Cloud<V> SelectMany<T, U, V>(this Cloud<T> workflow, Func<T, Cloud<U>> continuation, Func<T, U, V> projection)
        {
            return workflow.Then(t => continuation(t).Then(u => Cloud.Return(projection(t, u))));
        }

        public static Cloud<TResult> Select<TResult>(TResult value)
        {
            return Cloud.Return(value); 
        }

        private static void test ()
        {
            var wf = Cloud.Parallel(
                        Cloud.New(42),
                        Cloud.New(43))
                    .Then(xs => CloudAtom.New(xs.Sum()))
                    .Then(atom => CloudAtom.Read(atom));

            var foobar = (from x in Cloud.New(1)
                          from y in Cloud.New(42)
                          select x + y)
                         .Then(x => Cloud.New(x * x));
        }
    }
}
