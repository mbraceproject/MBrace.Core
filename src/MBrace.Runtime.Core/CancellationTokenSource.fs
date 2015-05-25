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

type ICancellationTokenSourceFactory =
    abstract CreateCancellationTokenSource : unit -> ICloudCancellationTokenSource
    abstract CreateLinkedCancellationTokenSource : parents:ICloudCancellationToken -> ICloudCancellationTokenSource


[<AutoOpen>]
module private DistributedCancellationTokenSourceImpl =

    open MBraceAsyncExtensions

    type DistributedCancellationTokenInfo =
        {
            IsCancellationRequested : bool
            Children : string list
        }

    /// cancels a distributed token entity and all its children
    let cancelTokenEntity (source : ICloudDictionary<DistributedCancellationTokenInfo>) (id : string) = async {
        let visited = new ConcurrentDictionary<string, unit> ()
        let rec cancel id = async {
            if visited.TryAdd(id, ()) then
                let! info = source.TryFind id
                match info with
                | Some info when not info.IsCancellationRequested ->
                    let! info = Async.OfCloud(source.Update(id, fun info -> { info with IsCancellationRequested = true }))
                    do! info.Children |> Seq.map cancel |> Async.Parallel |> Async.Ignore

                | _ -> return ()
        }

        do! cancel id
    }

    let tryCreateToken (source : ICloudDictionary<DistributedCancellationTokenInfo>) (parentId : string option) = async {
        let createEntry() = async {
            let id = mkUUID()
            do! Async.OfCloud(source.Add(id, { IsCancellationRequested = false ; Children = [] }))
            return id
        }

        match parentId with
        | None ->
            let! id = createEntry()
            return Some id

        | Some parentId ->
            let! parent = Async.OfCloud(source.TryFind parentId)
            match parent with
            | Some p when not p.IsCancellationRequested ->
                let! id = createEntry()

                let! parentInfo = Async.OfCloud(source.Update(parentId, fun info -> { info with Children = id :: info.Children }))

                // parent has been cancelled in the meantime, do a cancel of current token
                if parentInfo.IsCancellationRequested then do! cancelTokenEntity source id

                return Some id
            | _ -> return None
    }

    let checkCancellation (source : ICloudDictionary<DistributedCancellationTokenInfo>) (tokenId : string) = async {
        let! contents = Async.OfCloud(source.TryFind tokenId)
        return contents |> Option.forall (fun c -> c.IsCancellationRequested)
    }

    let localTokens = new System.Collections.Concurrent.ConcurrentDictionary<string, CancellationToken> ()
    /// creates a cancellation token that is updated by polling the token entities
    let createLocalCancellationToken id checkCanceled =
        let ok, t = localTokens.TryGetValue id
        if ok then t
        elif Async.RunSync checkCanceled then
            new CancellationToken(canceled = true)
        else
            let createToken _ =
                let cts = new System.Threading.CancellationTokenSource()

                let rec checkCancellation () = async {
                    let! isCancelled = Async.Catch checkCanceled
                    match isCancelled with
                    | Choice1Of2 true -> 
                        cts.Cancel()
                        localTokens.TryRemove id |> ignore
                    | Choice1Of2 false ->
                        do! Async.Sleep 200
                        return! checkCancellation ()
                    | Choice2Of2 _ ->
                        do! Async.Sleep 1000
                        return! checkCancellation ()
                }

                do Async.Start(checkCancellation())
                cts.Token

            localTokens.AddOrUpdate(id, createToken, fun _ t -> t)

/// Distributed ICloudCancellationTokenSource implementation
[<Sealed; DataContract>] 
type DistributedCancellationTokenSource private (source : ICloudDictionary<DistributedCancellationTokenInfo>, state : DistributedCancellationTokenState) =

    // A DistributedCancellationTokenSource has three possible internal states: Distributed, Local and Canceled
    // A local state is just an in-memory cancellation token without storage backing
    // Cancellation tokens can be elevated to distributed state by persisting to the table storage
    // This can happen either 1) upon creation 2) manually or 3) automatically on serialization of the object
    // This is to minimize communication for local-only cancellation tokens that can still acquire potentially global range.

    [<DataMember(Name = "Source")>]
    let source = source
    [<DataMember(Name = "State")>]
    let state = state

    // nonserializable cancellation token source that is initialized only in case of localized semantics
    [<IgnoreDataMember>]
    let localCancellationTokenSource =
        match state with
        | Localized None -> new CancellationTokenSource()
        | Localized (Some parent) -> CancellationTokenSource.CreateLinkedTokenSource [| parent.LocalToken |]
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
                        | Distributed id -> createLocalCancellationToken id (checkCancellation source id)

                    localToken <- Some ct
                    ct)

    /// Attempt to elevate cancellation token to global range
    member private c.TryElevateToDistributed() =
        match state with
        | Canceled -> None
        | Distributed rk -> Some rk
        | Localized _ ->
            lock state (fun () ->
                match state with
                | Canceled -> None
                | Distributed rk -> Some rk
                | Localized None ->
                    match tryCreateTokenEntity config partitionKey metadata None |> Async.RunSync with
                    | None -> state <- Canceled ; None
                    | Some rowKey -> state <- Distributed rowKey ; localToken <- None ; Some rowKey

                | Localized (metadata, Some parent) ->
                    // elevate parent to distributed source
                    match parent.TryElevateToDistributed() with
                    | None -> state <- Canceled ; None // parent canceled ; declare canceled
                    | Some parentRowKey ->
                        // create token entity using for current cts
                        match tryCreateTokenEntity config partitionKey metadata (Some (parent.PartitionKey, parentRowKey)) |> Async.RunSync with
                        | None -> state <- Canceled ; None
                        | Some rowKey -> state <- Distributed rowKey ; localToken <- None ; Some rowKey)

    member __.LocalToken = getLocalToken()


and private DistributedCancellationTokenState =
    | Canceled
    | Localized of parent:DistributedCancellationTokenSource option
    | Distributed of id:string