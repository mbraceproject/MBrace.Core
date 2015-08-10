namespace MBrace.Core

/// Denotes a distributable, serializable resource that can be disposed
/// in a global context. Types that implement ICloudDisposable
/// can be introduced in cloud { ... } workflows with the use and use! bindings.
type ICloudDisposable =
    /// Releases any storage resources used by this object.
    abstract Dispose : unit -> Async<unit>