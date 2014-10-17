namespace Nessos.MBrace

    /// Scheduling context for all tasks within current scope
    type SchedulingContext =
        /// Current thread scheduling context
        | Sequential
        /// Thread pool scheduling context
        | ThreadParallel
        /// Distributed scheduling context
        | Distributed

    /// Denotes a reference to a worker node in the cluster
    type IWorkerRef =
        /// Worker type identifier
        abstract Type : string
        /// Worker unique identifier
        abstract Id : string

    /// Denotes handle to a distributable resource that can be disposed of.
    type ICloudDisposable =
        /// Releases any storage resources used by this object.
        abstract Dispose : unit -> Async<unit>

    /// Exception raised on missing resource resolution
    exception ResourceNotFoundException of string
     with
        override e.Message = e.Data0