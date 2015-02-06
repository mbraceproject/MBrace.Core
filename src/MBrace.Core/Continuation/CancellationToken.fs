namespace MBrace

open System.Threading

/// Distributed cancellation token abstraction.
type ICloudCancellationToken =
    /// Gets the cancellation status for the token.
    abstract IsCancellationRequested : bool
    /// Gets a System.Threading.CancellationToken instance
    /// that is subscribed to the distributed cancellation token.
    abstract LocalToken : CancellationToken

/// Distributed cancellation token source abstraction.
type ICloudCancellationTokenSource =
    /// Cancel the cancellation token source.
    abstract Cancel : unit -> unit
    /// Gets a cancellation token instance.
    abstract Token : ICloudCancellationToken