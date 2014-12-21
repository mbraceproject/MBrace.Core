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
        public static Cloud<TResult> FromValue<TResult>(TResult value)
        {
            return builder.Return(value);
        }

        public static Cloud<TResult> AsCloud<TResult>(this TResult value)
        {
            return builder.Return(value);
        }

        public static Cloud<TResult> New<TResult>(Func<Cloud<TResult>> delay)
        {
            return builder.Delay(delay.AsFSharpFunc());
        }

        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, Cloud<TResult>> continuation)
        {
            var fsFunc = continuation.AsFSharpFunc();
            return builder.Bind<TSource, TResult>(workflow, fsFunc);
        }

        public static Cloud<TResult> ReturnFrom<TResult>(this Cloud<TResult> workflow)
        {
            return builder.ReturnFrom<TResult>(workflow);
        }

        // Linq comprehension syntax friendly methods.

        public static Cloud<V> SelectMany<T, U, V>(this Cloud<T> workflow, Func<T, Cloud<U>> continuation, Func<T, U, V> projection)
        {
            return workflow.Then(t => continuation(t).Then(u => Cloud.FromValue(projection(t, u))));
        }

        public static Cloud<TResult> Select<TResult>(TResult value)
        {
            return Cloud.FromValue(value); 
        }
    }
}
