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


//
//  Wrapper implementations for System.Threading.CancellationToken
//

[<AutoSerializable(false)>]
type internal InMemoryCancellationToken (token : CancellationToken) =
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested
        member __.LocalToken = token

[<AutoSerializable(false)>]
type internal InMemoryCancellationTokenSource (cts : CancellationTokenSource) =
    interface ICloudCancellationTokenSource with
        member __.Cancel() = cts.Cancel()
        member __.Token = new InMemoryCancellationToken(cts.Token) :> _

    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) =
        let ltokens = tokens |> Seq.map (fun t -> t.LocalToken) |> Seq.toArray

        let lcts =
            if Array.isEmpty ltokens then new CancellationTokenSource()
            else
                CancellationTokenSource.CreateLinkedTokenSource ltokens

        new InMemoryCancellationTokenSource(lcts)