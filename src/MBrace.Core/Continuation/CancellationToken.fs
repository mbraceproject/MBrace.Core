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


namespace MBrace.Runtime.InMemory

open System.Threading

open MBrace

[<AutoSerializable(false)>]
type InMemoryCancellationToken (token : CancellationToken) =
    new () = new InMemoryCancellationToken(new CancellationToken())
    member __.LocalToken = token
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested
        member __.LocalToken = token

[<AutoSerializable(false)>]
type InMemoryCancellationTokenSource (cts : CancellationTokenSource) =
    let token = new InMemoryCancellationToken(cts.Token)
    new () = new InMemoryCancellationTokenSource(new CancellationTokenSource())
    member __.Token = token
    member __.Cancel() = cts.Cancel()
    interface ICloudCancellationTokenSource with
        member __.Cancel() = cts.Cancel()
        member __.Token = token :> _

    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) =
        let ltokens = tokens |> Seq.map (fun t -> t.LocalToken) |> Seq.toArray
        let lcts =
            if Array.isEmpty ltokens then new CancellationTokenSource()
            else
                CancellationTokenSource.CreateLinkedTokenSource ltokens

        new InMemoryCancellationTokenSource(lcts)