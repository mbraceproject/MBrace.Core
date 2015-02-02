using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MCloud = MBrace.Cloud;

namespace MBrace.CSharp
{
    public static partial class Cloud
    {
        #region Cloud.Parallel
        /// <summary>
        ///     Executes a collection of workflows using parallel fork/join semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed in parallel.</param>
        /// <returns>Array of aggregated results.</returns>
        public static Cloud<TResult[]> Parallel<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            var wfs = workflows.Select(w => w).ToArray();
            return MCloud.Parallel(wfs);
        }

        /// <summary>
        ///     Executes a collection of workflows using parallel fork/join semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed in parallel.</param>
        /// <returns>Array of aggregated results.</returns>
        public static Cloud<TResult[]> Parallel<TResult>(params Cloud<TResult>[] workflows)
        {
            return Cloud.Parallel((IEnumerable<Cloud<TResult>>)workflows);
        }

        /// <summary>
        ///     Creates a cloud computation that will execute provided computation on every available worker
        ///     in the cluster and if successful returns the array of gathered results.
        ///     This operator may create distribution.
        ///     Any exception raised by children carry cancellation semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflow"></param>
        /// <returns>Array of the aggregated results.</returns>
        public static Cloud<TResult[]> Parallel<TResult>(Cloud<TResult> workflow)
        {
            return MCloud.Parallel(workflow);
        }

        /// <summary>
        ///     Creates a cloud computation that will execute given computations to targeted workers
        ///     possibly in parallel and if successful returns the array of gathered results.
        ///     This operator may create distribution.
        ///     Exceptions raised by children carry cancellation semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed in parallel in each worker.</param>
        /// <returns>Array of the aggregated results.</returns>
        public static Cloud<TResult []> Parallel<TResult>(this IEnumerable<Tuple<Cloud<TResult>, IWorkerRef>> workflows) 
        {
            var wfs = workflows.ToArray();
            return MCloud.Parallel(wfs);
        }

        #endregion

        #region Cloud.Choice
        /// <summary>
        ///     Performs a nondeterministic computation in parallel.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed nondeterministically.</param>
        /// <returns>The result of the first computation to complete in the positive.</returns>
        public static Cloud<Option<TResult>> Choice<TResult>(this IEnumerable<Cloud<Option<TResult>>> workflows)
        {
            Func<Option<TResult>, MBrace.Cloud<FSharpOption<TResult>>> transform = option => Builder.Return(option.AsFSharpOption());
            var fsTransform = transform.AsFSharpFunc();

            var wfs = workflows
                        .Select(wf => Builder.Bind(wf, fsTransform))
                        .ToArray();
            var choice = MCloud.Choice<TResult>(wfs);

            Func<FSharpOption<TResult>, MBrace.Cloud<Option<TResult>>> transformRev = fsOption => Builder.Return(Option<TResult>.FromFSharpOption(fsOption));
            var fsTransformRev = transformRev.AsFSharpFunc();
            var result = Builder.Bind(choice, fsTransformRev);

            return result;
        }

        /// <summary>
        ///     Performs a nondeterministic computation in parallel.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed nondeterministically.</param>
        /// <returns>The result of the first computation to complete in the positive.</returns>
        public static Cloud<Option<TResult>> Choice<TResult>(params Cloud<Option<TResult>>[] workflows)
        {
            return Cloud.Choice((IEnumerable<Cloud<Option<TResult>>>)workflows);
        }

        /// <summary>
        ///     Performs a nondeterministic computation in parallel.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed nondeterministically.</param>
        /// <returns>The result of the first computation to complete.</returns>
        public static Cloud<TResult> Choice<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            if (!workflows.Any()) throw new ArgumentException("Workflows sequence is empty.");

            return workflows
                    .Select(wf => wf.Then(w => Option<TResult>.Some(w).AsCloud()))
                    .Choice()
                    .Then(result => result.Value.AsCloud());
        }

        /// <summary>
        ///     Performs a nondeterministic computation in parallel.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed nondeterministically.</param>
        /// <returns>The result of the first computation to complete in the positive.</returns>
        public static Cloud<TResult> Choice<TResult>(params Cloud<TResult>[] workflows)
        {
            return Cloud.Choice((IEnumerable<Cloud<TResult>>)workflows);
        }

        /// <summary>
        ///     Returns a cloud computation that will execute the given computation on every available worker
        ///     possibly in parallel and will return when any of the supplied computations
        ///     have returned a successful value or if all of them fail to succeed. 
        ///     If a computation succeeds the rest of them are canceled.
        ///     The success of a computation is encoded as an option type.
        ///     This operator may create distribution.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflow">Input workflow to be executed nondeterministically everywhere.</param>
        /// <returns>The result of the first computation to complete in the positive.</returns>
        public static Cloud<Option<TResult>> Choice<TResult>(Cloud<Option<TResult>> workflow)
        {
            Func<Option<TResult>, MBrace.Cloud<FSharpOption<TResult>>> transform = option => Builder.Return(option.AsFSharpOption());
            var fsTransform = transform.AsFSharpFunc();
            var x = Builder.Bind(workflow, fsTransform);
            
            var choice = MCloud.Choice(x);
            
            Func<FSharpOption<TResult>, MBrace.Cloud<Option<TResult>>> transformRev = fsOption => Builder.Return(Option<TResult>.FromFSharpOption(fsOption));
            var fsTransformRev = transformRev.AsFSharpFunc();
            var result = Builder.Bind(choice, fsTransformRev);

            return result;
        }

        /// <summary>
        ///     Returns a cloud computation that will execute the given computation on the corresponding worker,
        ///     possibly in parallel and will return when any of the supplied computations
        ///     have returned a successful value or if all of them fail to succeed. 
        ///     If a computation succeeds the rest of them are canceled.
        ///     The success of a computation is encoded as an option type.
        ///     This operator may create distribution.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed nondeterministically.</param>
        /// <returns>The result of the first computation to complete in the positive.</returns>
        public static Cloud<Option<TResult>> Choice<TResult>(this IEnumerable<Tuple<Cloud<Option<TResult>>, IWorkerRef>> workflows)
        {
            Func<Option<TResult>, MBrace.Cloud<FSharpOption<TResult>>> transform = option => Builder.Return(option.AsFSharpOption());
            var fsTransform = transform.AsFSharpFunc();

            var wfs = workflows
                        .Select(wf => Tuple.Create(Builder.Bind(wf.Item1, fsTransform), wf.Item2))
                        .ToArray();
            var choice = MCloud.Choice<TResult>(wfs);

            Func<FSharpOption<TResult>, MBrace.Cloud<Option<TResult>>> transformRev = fsOption => Builder.Return(Option<TResult>.FromFSharpOption(fsOption));
            var fsTransformRev = transformRev.AsFSharpFunc();
            var result = Builder.Bind(choice, fsTransformRev);

            return result;
        }
        #endregion
    }
}
