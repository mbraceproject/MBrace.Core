namespace MBrace.Runtime

open System
open System.Threading
open System.Runtime.Serialization
open System.Collections.Generic
open System.Collections.Concurrent

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Utils

type private LocalCancellationTokenManager () =
    let localTokens = new ConcurrentDictionary<string, CancellationToken> ()
    let globalCts = new CancellationTokenSource()
    
    /// <summary>
    ///     Gets a System.Threading.Cancellation token linked to the cloud cancellation token entry.
    /// </summary>
    /// <param name="token">Cancellation token entry to be proxied.</param>
    member __.GetLocalCancellationToken(token : ICancellationEntry) : Async<CancellationToken> = async {
        let ok, t = localTokens.TryGetValue token.UUID
        if ok then return t else
        let! isCancelled = token.IsCancellationRequested
        if isCancelled then
            return new CancellationToken(canceled = true)
        else
            let createToken _ =
                let cts = new CancellationTokenSource()

                let rec checkCancellation () = async {
                    let! isCancelled = Async.Catch token.IsCancellationRequested
                    match isCancelled with
                    | Choice1Of2 true -> 
                        cts.Cancel()
                        localTokens.TryRemove token.UUID |> ignore
                    | Choice1Of2 false ->
                        do! Async.Sleep 200
                        return! checkCancellation ()
                    | Choice2Of2 _ ->
                        do! Async.Sleep 1000
                        return! checkCancellation ()
                }

                do Async.Start(checkCancellation(), cancellationToken = globalCts.Token)
                cts.Token

            return localTokens.AddOrUpdate(token.UUID, createToken, fun _ t -> t)
    }

    interface IDisposable with
        member __.Dispose () = globalCts.Cancel()

[<NoEquality; NoComparison>]
type private CloudCancellationTokenState =
    | Canceled
    | Localized of parents:CloudCancellationToken []
    | Distributed of token:ICancellationEntry

/// Distributed ICloudCancellationToken[Source] implementation
and [<Sealed; DataContract; NoEquality; NoComparison>]
    CloudCancellationToken private (source : ICancellationEntryFactory, state : CloudCancellationTokenState) =

    static let manager = new LocalCancellationTokenManager ()

    // A CloudCancellationToken has three possible internal states: Distributed, Localized and Canceled
    // A local representation is just an in-memory cancellation token without storage backing
    // Cancellation tokens can be elevated to distributed state by persisting to table storage
    // This can happen either 1) upon creation 2) manually or 3) automatically on first serialization of the object
    // This is to minimize communication for local-only cancellation tokens that can still acquire potentially global visibility.

    [<DataMember(Name = "Source")>]
    let source = source
    [<DataMember(Name = "State")>]
    let mutable state = state

    // nonserializable cancellation token source that is initialized only in case of localized semantics
    [<IgnoreDataMember>]
    let localCancellationTokenSource =
        match state with
        | Localized [||] -> new CancellationTokenSource()
        | Localized parents -> CancellationTokenSource.CreateLinkedTokenSource (parents |> Array.map (fun p -> p.LocalToken))
        | _ -> null

    // local cancellation token instace for source    
    [<IgnoreDataMember>]
    let mutable localToken : CancellationToken option = None

    // resolves local token for the instance
    let getLocalToken () =
        match localToken with
        | Some ct -> ct
        | None ->
            lock state (fun () ->
                match localToken with
                | Some ct -> ct
                | None ->
                    let ct =
                        match state with
                        | Canceled -> new CancellationToken(canceled = true)
                        | Localized _ -> localCancellationTokenSource.Token
                        | Distributed token -> manager.GetLocalCancellationToken(token) |> Async.RunSync

                    localToken <- Some ct
                    ct)

    /// Attempt to elevate cancellation token to global range
    member private c.TryElevateToDistributed() =
        match state with
        | Canceled -> None
        | Distributed token -> Some token
        | Localized _ ->
            lock state (fun () ->
                match state with
                | Canceled -> None
                | Localized _ when localCancellationTokenSource.IsCancellationRequested -> None
                | Localized [||] ->
                    let token = source.CreateCancellationEntry() |> Async.RunSync
                    state <- Distributed token ; localToken <- None ; 
                    Some token

                | Localized parents ->
                    // elevate parents to distributed source
                    let elevatedParents = parents |> Array.Parallel.map (fun p -> p.TryElevateToDistributed()) |> Array.choose id

                    if Array.isEmpty elevatedParents then
                        state <- Canceled ; None
                    else
                        // create token entity using for current cts
                        match source.TryCreateLinkedCancellationEntry elevatedParents |> Async.RunSync with
                        | None -> state <- Canceled ; None
                        | Some id -> state <- Distributed id ; localToken <- None ; Some id

                | Distributed token -> Some token)

    /// Triggers elevation in event of serialization
    [<OnSerializing>]
    member private c.OnDeserializing (_ : StreamingContext) = c.TryElevateToDistributed() |> ignore

    /// Elevates cancellation token to global scope. Returns true if succesful, false if already canceled.
    member __.ElevateToGlobal () = __.TryElevateToDistributed() |> Option.isSome

    /// Elevated cancellation token id
    member __.GlobalId =
        match state with
        | Distributed id -> Some id.UUID
        | _ -> None

    /// System.Threading.Token for distributed cancellation token
    member __.LocalToken = getLocalToken()

    /// Cancel cancellation token
    member __.Cancel() =
        if (let t = __.LocalToken in t.IsCancellationRequested) then ()
        else
            lock state (fun () ->
                match state with
                | Canceled _ -> ()
                | Localized _ -> localCancellationTokenSource.Cancel()
                | Distributed token -> token.Cancel() |> Async.RunSync)

    interface ICloudCancellationToken with
        member x.IsCancellationRequested: bool = let t = getLocalToken() in t.IsCancellationRequested
        member x.LocalToken: CancellationToken = getLocalToken()

    interface ICloudCancellationTokenSource with
        member x.Cancel(): unit = x.Cancel()
        member x.Token: ICloudCancellationToken = (x :> ICloudCancellationToken)

    /// <summary>
    ///     Creates a distributed cancellation token source with lazy elevation to distributed cancellation semantics.
    /// </summary>
    /// <param name="source">Backend source for distributed cancellation tokens.</param>
    /// <param name="parents">Parent distributed cancellation token sources.</param>
    /// <param name="elevate">Directly elevate cancellation token to distributed. Defaults to false.</param>
    static member Create(source : ICancellationEntryFactory, ?parents : ICloudCancellationToken [], ?elevate : bool) = async {
        let elevate = defaultArg elevate false
        let parents = 
            try defaultArg parents [||] |> Array.map unbox
            with :? InvalidCastException -> invalidArg "parents" "Passed invalid cloud cancellation token instance."

        match parents with
        | [||] when elevate ->
            let! token = source.CreateCancellationEntry()
            return new CloudCancellationToken(source, Distributed token)
        | _ ->
            let dcts = new CloudCancellationToken(source, Localized(parents))
            if elevate then dcts.TryElevateToDistributed() |> ignore
            return dcts
    }