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
        /// <summary>
        ///     Executes a collection of workflows using parallel fork/join semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed in parallel.</param>
        /// <returns>Array of aggregated results.</returns>
        public static Cloud<TResult[]> Parallel<TResult>(this IEnumerable<Cloud<TResult>> workflows)
        {
            var wfs = workflows.Select(w => w.Computation).ToArray();
            return new Cloud<TResult[]>(MCloud.Parallel(wfs));
        }

        /// <summary>
        ///     Executes a collection of workflows using parallel fork/join semantics.
        /// </summary>
        /// <typeparam name="TResult">Computation return type.</typeparam>
        /// <param name="workflows">Input workflows to be executed in parallel.</param>
        /// <returns>Array of aggregated results.</returns>
        public static Cloud<TResult[]> Parallel<TResult>(params Cloud<TResult> [] workflows)
        {
            return Cloud.Parallel((IEnumerable<Cloud<TResult>>)workflows);
        }

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
                        .Select(wf => Builder.Bind(wf.Computation, fsTransform))
                        .ToArray();
            var choice = MCloud.Choice<TResult>(wfs);

            Func<FSharpOption<TResult>, MBrace.Cloud<Option<TResult>>> transformRev = fsOption => Builder.Return(Option<TResult>.FromFSharpOption(fsOption));
            var fsTransformRev = transformRev.AsFSharpFunc();
            var result = Builder.Bind(choice, fsTransformRev);

            return new Cloud<Option<TResult>>(result);
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
        public static Cloud<TResult> Choice<TResult>(params Cloud<TResult> [] workflows)
        {
            return Cloud.Choice((IEnumerable<Cloud<TResult>>)workflows);
        }
    }
}
