namespace Nessos.MBrace

    /// Distributed reference to value stored in the cloud.
    type ICloudRef<'T> =
        inherit ICloudDisposable
        /// Cloud ref identifier.
        abstract Id : string
        /// Asynchronously dereferences the cloud ref.
        abstract GetValue : unit -> Async<'T>