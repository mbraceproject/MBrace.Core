namespace MBrace.Core

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
    inherit ICloudDisposable
    /// Cancel the cancellation token source.
    abstract Cancel : unit -> unit
    /// Gets a cancellation token instance.
    abstract Token : ICloudCancellationToken

/// Simple proxy to System.Threading.CancellationToken
type internal InMemoryCancellationToken(ct : CancellationToken) =
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = ct.IsCancellationRequested
        member __.LocalToken = ct