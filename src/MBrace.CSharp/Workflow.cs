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
            return Builder.Return(value);
        }

        /// <summary>
        ///     Wraps given value in cloud workflow that returns it.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="value">Value to be wrapped.</param>
        /// <returns>A cloud workflow that will return the value once executed.</returns>
        public static Cloud<TResult> AsCloud<TResult>(this TResult value)
        {
            return Builder.Return(value);
        }

        /// <summary>
        /// Creates a cloud workflow that throws an exception.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="ex">The exception to throw.</param>
        /// <returns>Throws given exception.</returns>
        public static Cloud<TResult> Throw<TResult>(Exception ex)
        {
            return MBrace.Cloud.Raise<TResult>(ex);
        }

        /// <summary>
        /// Try/Finally workflow.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="body">The computation to execute.</param>
        /// <param name="finally">Finalizer.</param>
        public static Cloud<TResult> TryFinally<TResult>(Cloud<TResult> body, CloudAction @finally)
        {
            return MBrace.Cloud.TryFinally(body, @finally.Body);
        }

        /// <summary>
        ///     Wraps a delayed cloud workflow in a containing workflow.
        /// </summary>
        /// <typeparam name="TResult">Function return type.</typeparam>
        /// <param name="func">Delayed cloud workflow.</param>
        /// <returns>A cloud workflow that wraps the delayed cloud workflow.</returns>
        public static Cloud<TResult> New<TResult>(Func<Cloud<TResult>> func)
        {
            return Builder.Delay(func.AsFSharpFunc());
        }

        /// <summary>
        ///     Wraps given function in cloud workflow that executes it.
        /// </summary>
        /// <typeparam name="TResult">Function return type.</typeparam>
        /// <param name="func">Function to be wrapped.</param>
        /// <returns>A cloud workflow that will call the function and return its result once executed.</returns>
        public static Cloud<TResult> New<TResult>(Func<TResult> func)
        {
            Func<Cloud<TResult>> cloudDelay = () => func().AsCloud();
            return Builder.Delay(cloudDelay.AsFSharpFunc());
        }

        /// <summary>
        ///     Wraps given function in cloud workflow that executes it.
        /// </summary>
        /// <param name="delay">Function to be wrapped.</param>
        /// <returns>A cloud workflow that will call the function once executed.</returns>
        public static CloudAction New(Func<CloudAction> delay)
        {
            Func<Cloud<Unit>> f = () => delay().Body;
            var wf = Builder.Delay(f.AsFSharpFunc());
            return new CloudAction(wf);
        }

        /// <summary>
        ///     Creates a cloud workflow that binds a workflow to a callback.
        /// </summary>
        /// <typeparam name="TSource">Source type.</typeparam>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="workflow">Initial workflow to be executed.</param>
        /// <param name="continuation">Callback workflow to be executed on completion of the former.</param>
        /// <returns>A combined workflow.</returns>
        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, Cloud<TResult>> continuation)
        {
            return Builder.Bind<TSource, TResult>(workflow, continuation.AsFSharpFunc());
        }

        /// <summary>
        ///     Creates a cloud workflow that binds a workflow to a callback.
        /// </summary>
        /// <typeparam name="TSource">Source type.</typeparam>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="workflow">Initial workflow to be executed.</param>
        /// <param name="continuation">Callback workflow to be executed on completion of the former.</param>
        /// <returns>A combined workflow.</returns>
        public static Cloud<TResult> Then<TSource, TResult>(this Cloud<TSource> workflow, Func<TSource, TResult> continuation)
        {
            Func<TSource, Cloud<TResult>> f = x => continuation(x).AsCloud();
            return Builder.Bind<TSource, TResult>(workflow, f.AsFSharpFunc());
        }

        /// <summary>
        ///     Creates a cloud workflow that binds a workflow to a callback.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="workflow">Initial workflow to be executed.</param>
        /// <param name="continuation">Callback workflow to be executed on completion of the former.</param>
        /// <returns>A combined workflow.</returns>
        public static Cloud<TResult> Then<TResult>(this CloudAction workflow, Func<TResult> continuation)
        {
            Func<Cloud<TResult>> f = () => continuation().AsCloud();
            return Builder.Bind<Unit, TResult>(workflow.Body, f.AsFSharpFunc());
        }

        /// <summary>
        ///     Creates a cloud workflow that binds a workflow to a callback.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="workflow">Initial workflow to be executed.</param>
        /// <param name="continuation">Callback workflow to be executed on completion of the former.</param>
        /// <returns>A combined workflow.</returns>
        public static Cloud<TResult> Then<TResult>(this CloudAction workflow, Func<Cloud<TResult>> continuation)
        {
            return Builder.Bind<Unit, TResult>(workflow.Body, continuation.AsFSharpFunc());
        }



        // Linq comprehension syntax friendly methods.

        /// <summary>
        ///     Creates a cloud workflow that chains a group of callbacks together.
        /// </summary>
        /// <typeparam name="T">Initial return type.</typeparam>
        /// <typeparam name="U">Secondary return type.</typeparam>
        /// <typeparam name="V">Final result type.</typeparam>
        /// <param name="workflow">Initial workflow to execute.</param>
        /// <param name="continuation">Callback to primary and secondary result values.</param>
        /// <param name="projection">Result reducing function.</param>
        /// <returns>A Cloud workflow </returns>
        public static Cloud<V> SelectMany<T, U, V>(this Cloud<T> workflow, Func<T, Cloud<U>> continuation, Func<T, U, V> projection)
        {
            return workflow.Then(t => continuation(t).Then(u => Cloud.FromValue(projection(t, u))));
        }

        /// <summary>
        ///     Wraps given value in cloud workflow that returns it.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="value">Value to be wrapped.</param>
        /// <returns>A cloud workflow that will return the value once executed.</returns>
        public static Cloud<TResult> Select<TResult>(TResult value)
        {
            return Cloud.FromValue(value); 
        }
    }
}
