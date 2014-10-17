namespace Nessos.MBrace

    /// Denotes handle to a distributable resource that can be disposed of.
    type ICloudDisposable =
        /// Releases any storage resources used by this object.
        abstract Dispose : unit -> Async<unit>

    /// Denotes a reference to a worker node in the cluster
    type IWorkerRef =
        /// Worker unique identifier
        abstract Id : string

    /// Record containing cluster information
    type RuntimeInfo =
        {
            /// Cloud process id
            ProcessId : string
            /// Cloud task id
            TaskId : string
            /// Available worker nodes
            Workers : IWorkerRef []
            /// Current worker node
            CurrentWorker : IWorkerRef
        }

    /// Cloud workflow execution context
    type SchedulingContext =
        | Sequential
        | ThreadParallel
        | Distributed

    /// Exception raised on missing resource resolution
    exception ResourceNotFoundException of string
     with
        override e.Message = e.Data0