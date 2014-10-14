namespace Nessos.MBrace

    open System.Runtime.Serialization

    /// Denotes handle to a distributable resource that can be disposed of.
    type ICloudDisposable =
        inherit ISerializable
        /// Releases any storage resources used by this object.
        abstract Dispose : unit -> Async<unit>