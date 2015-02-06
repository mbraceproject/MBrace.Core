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
        ///     Writes the following message to MBrace logging interface.
        /// </summary>
        /// <param name="format">Format string.</param>
        /// <param name="args">Arguments to format string.</param>
        public static CloudAction Log(string format, params object[] args)
        {
            return new CloudAction(MCloud.Log(String.Format(format, args)));
        }

        /// <summary>
        ///     Asynchronously suspends workflow for given amount of milliseconds.
        /// </summary>
        /// <param name="millisecondsDue">Milliseconds to suspend computation.</param>
        public static CloudAction Sleep(int millisecondsDue)
        {
            return new CloudAction(MCloud.Sleep(millisecondsDue));
        }

        /// <summary>
        /// Gets total number of available workers in cluster context.
        /// </summary>
        /// <returns>The number of workers.</returns>
        public static Cloud<int> GetWorkerCount()
        {
            return MCloud.GetWorkerCount();
        }

        /// <summary>
        /// Await for task completion.
        /// </summary>
        /// <typeparam name="TResult">Return type of task.</typeparam>
        /// <param name="task">Task to be awaited.</param>
        public static Cloud<TResult> AwaitTask<TResult>(Task<TResult> task)
        {
            return MCloud.AwaitTask(task, null); 
        }

        /// <summary>
        /// Gets current worker.
        /// </summary>
        public static Cloud<IWorkerRef> CurrentWorker
        {
            get { return MCloud.CurrentWorker; }
        }

        /// <summary>
        /// Disposes of a distributed resource.
        /// </summary>
        /// <param name="disposable">The resource to be disposed.</param>
        public static CloudAction Dispose<Disposable>(this Disposable disposable) where Disposable : ICloudDisposable
        {
            return new CloudAction(MCloud.Dispose(disposable));
        }

        /// <summary>
        /// Get all workers in currently running cluster context.
        /// </summary>
        public static Cloud<IWorkerRef []> GetAvailableWorkers()
        {
            return MCloud.GetAvailableWorkers(); 
        }

        /// <summary>
        /// Gets the assigned id of the currently running cloud process.
        /// </summary>
        public static Cloud<string> GetProcessId()
        {
            return MCloud.GetProcessId(); 
        }

        /// <summary>
        /// Gets the assigned id of the currently running cloud task.
        /// </summary>
        public static Cloud<string> GetTaskId()
        {
            return MCloud.GetTaskId(); 
        }

        /// <summary>
        /// Performs cloud computation discarding its result.
        /// </summary>
        /// <typeparam name="TResult">Return type of the given workflow.</typeparam>
        /// <param name="workflow">The workflow to ignore.</param>
        public static CloudAction Ignore<TResult>(this Cloud<TResult> workflow)
        {
            return new CloudAction(MCloud.Ignore(workflow));
        }

        /// <summary>
        /// Returns true iff runtime supports executing workflows in specific worker.
        /// Should be used with combinators that support worker targeting like Cloud.Parallel/Choice/StartChild.
        /// </summary>
        public static Cloud<bool> IsTargetedWorkerSupported
        {
            get { return MCloud.IsTargetedWorkerSupported; }
        }
    }
}
