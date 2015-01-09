using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp
{
    /// <summary>
    /// Provides methods to create basic Cloud workflows.
    /// </summary>
    public static partial class Cloud
    {
        internal static CloudBuilder Builder = CloudBuilderModule.cloud;

        /// <summary>
        /// Constructs a cloud computation that returns the given value.
        /// </summary>
        /// <typeparam name="TResult">Type of the computation.</typeparam>
        /// <param name="value">Value to return.</param>
        public static Cloud<TResult> FromValue<TResult>(TResult value)
        {
            return new Cloud<TResult>(Builder.Return(value));
        }

        public static Cloud<TResult> AsCloud<TResult>(this TResult value)
        {
            return new Cloud<TResult>(Builder.Return(value));
        }

        public static Cloud<TResult> New<TResult>(Func<Cloud<TResult>> delay)
        {
            return new Cloud<TResult>(Builder.Delay(delay.AsFSharpFunc()));
        }

        public static CloudUnit New(Func<CloudUnit> delay)
        {
            return new CloudUnit(Builder.Delay(delay.AsFSharpFunc()));
        }

        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, Cloud<TResult>> continuation)
        {
            return new Cloud<TResult>(Builder.Bind<TSource, TResult>(workflow.Computation, continuation.AsFSharpFunc()));
        }

        public static Cloud<TResult> Then<TResult>(this CloudUnit workflow, Func<Cloud<TResult>> continuation)
        {
            return new Cloud<TResult>(Builder.Bind<Unit, TResult>(workflow.Computation, continuation.AsFSharpFunc()));
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
