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
    abstract CreateCancellationTokenSource : unit -> Async<ICloudCancellationTokenSource>
    abstract CreateLinkedCancellationTokenSource : parent:ICloudCancellationToken -> Async<ICloudCancellationTokenSource>

type internal DistributedCancellationTokenInfo =
    {
        IsCancellationRequested : bool
        Children : string list
    }

[<AutoOpen>]
module private DistributedCancellationTokenSourceImpl =

    open MBraceAsyncExtensions

    /// check cancellation 
    let checkCancellation (source : ICloudDictionary<DistributedCancellationTokenInfo>) (tokenId : string) = async {
        let! contents = source.TryFind tokenId
        return
            match contents with
            | None -> true
            | Some c -> c.IsCancellationRequested
    }

    /// cancels a distributed token entity and all its children
    let cancelTokenEntity (source : ICloudDictionary<DistributedCancellationTokenInfo>) (id : string) = async {
        let visited = new ConcurrentDictionary<string, unit> ()
        let rec cancel id = async {
            if visited.TryAdd(id, ()) then
                let! isCancelled = checkCancellation source id
                if isCancelled then
                    let! info = source.Update(id, fun info -> { info with IsCancellationRequested = true })
                    do! info.Children |> Seq.map cancel |> Async.Parallel |> Async.Ignore
        }

        do! cancel id
    }

    /// creates a new cancellation token id with provided parent id
    let tryCreateToken (source : ICloudDictionary<DistributedCancellationTokenInfo>) (parentId : string option) = async {
        let createEntry() = async {
            let id = mkUUID()
            do! source.Add(id, { IsCancellationRequested = false ; Children = [] })
            return id
        }

        match parentId with
        | None ->
            let! id = createEntry()
            return Some id

        | Some parentId ->
            let! parent = source.TryFind parentId
            match parent with
            | Some p when not p.IsCancellationRequested ->
                let! id = createEntry()

                let! parentInfo = source.Update(parentId, fun info -> { info with Children = id :: info.Children })

                // parent has been cancelled in the meantime, do a cancel of current token
                if parentInfo.IsCancellationRequested then do! cancelTokenEntity source id

                return Some id
            | _ -> return None
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


[<NoEquality; NoComparison>]
type private DistributedCancellationTokenState =
    | Canceled
    | Localized of parent:DistributedCancellationToken option
    | Distributed of id:string

/// Distributed ICloudCancellationTokenSource implementation
and [<Sealed; DataContract; NoEquality; NoComparison>] 
    DistributedCancellationToken private (source : ICloudDictionary<DistributedCancellationTokenInfo>, state : DistributedCancellationTokenState) =

    // A DistributedCancellationTokenSource has three possible internal states: Distributed, Local and Canceled
    // A local state is just an in-memory cancellation token without storage backing
    // Cancellation tokens can be elevated to distributed state by persisting to the table storage
    // This can happen either 1) upon creation 2) manually or 3) automatically on serialization of the object
    // This is to minimize communication for local-only cancellation tokens that can still acquire potentially global range.

    [<DataMember(Name = "Source")>]
    let source = source
    [<DataMember(Name = "State")>]
    let mutable state = state

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
                | Distributed id -> Some id
                | Localized None ->
                    match tryCreateToken source None |> Async.RunSync with
                    | None -> state <- Canceled ; None
                    | Some id -> state <- Distributed id ; localToken <- None ; Some id

                | Localized (Some parent) ->
                    // elevate parent to distributed source
                    match parent.TryElevateToDistributed() with
                    | None -> state <- Canceled ; None // parent canceled ; declare canceled
                    | Some parentId ->
                        // create token entity using for current cts
                        match tryCreateToken source (Some parentId) |> Async.RunSync with
                        | None -> state <- Canceled ; None
                        | Some id -> state <- Distributed id ; localToken <- None ; Some id)

    /// Triggers elevation in event of serialization
    [<OnSerializing>]
    member private c.OnDeserializing (_ : StreamingContext) = c.TryElevateToDistributed() |> ignore

    /// Elevates cancellation token to global scope. Returns true if succesful, false if already canceled.
    member __.ElevateCancellationToken () = __.TryElevateToDistributed() |> Option.isSome

    /// Elevated cancellation token id
    member __.CancellationId =
        match state with
        | Distributed id -> Some id
        | _ -> None

    /// System.Threading.Token for distributed cancellation token
    member __.LocalToken = getLocalToken()
    /// Returns true if has been cancelled
    member __.IsCancellationRequested = let t = getLocalToken() in t.IsCancellationRequested
    /// Cancel cancellation token
    member __.Cancel() =
        if (let t = getLocalToken() in t.IsCancellationRequested) then ()
        else
            lock state (fun () ->
                match state with
                | Canceled _ -> ()
                | Localized _ -> localCancellationTokenSource.Cancel()
                | Distributed id -> cancelTokenEntity source id |> Async.RunSync)

    member internal __.Source = source

    interface ICloudCancellationToken with
        member this.IsCancellationRequested : bool = this.IsCancellationRequested
        member this.LocalToken : CancellationToken = getLocalToken ()

    interface ICloudCancellationTokenSource with
        member this.Cancel() : unit = this.Cancel()
        member this.Token : ICloudCancellationToken = this :> ICloudCancellationToken

    /// <summary>
    ///     Creates a distributed cancellation token source with lazy elevation to distributed cancellation semantics.
    /// </summary>
    /// <param name="source">Backend source for distributed cancellation tokens.</param>
    /// <param name="parent">Parent distributed cancellation token source. Defaults to no parent.</param>
    /// <param name="elevate">Directly elevate cancellation token to distributed. Defaults to false.</param>
    static member internal Create(source : ICloudDictionary<DistributedCancellationTokenInfo>, ?parent : DistributedCancellationToken, ?elevate : bool) = async {
        let elevate = defaultArg elevate false
        match parent with
        | None when elevate ->
            let! id = tryCreateToken source None
            return new DistributedCancellationToken(source, Distributed <| Option.get id)
        | _ ->
            let dcts = new DistributedCancellationToken(source, Localized(parent))
            if elevate then dcts.TryElevateToDistributed() |> ignore
            return dcts
    }

and [<Sealed;DataContract>] DistributedCancellationTokenFactory private (source : ICloudDictionary<DistributedCancellationTokenInfo>) =
    
    [<DataMember(Name = "Source")>]
    let source = source

    /// <summary>
    ///     Creates a distributed cancellation token source with lazy elevation to distributed cancellation semantics.
    /// </summary>
    /// <param name="source">Backend source for distributed cancellation tokens.</param>
    /// <param name="parent">Parent distributed cancellation token source. Defaults to no parent.</param>
    /// <param name="elevate">Directly elevate cancellation token to distributed. Defaults to false.</param>
    member __.Create(?parent : DistributedCancellationToken, ?elevate : bool) =
        DistributedCancellationToken.Create(source, ?parent = parent, ?elevate = elevate)

    static member Create(dictionaryProvider : ICloudDictionaryProvider) = async {
        let! dict = dictionaryProvider.Create<DistributedCancellationTokenInfo>()
        return new DistributedCancellationTokenFactory(dict)
    }

    interface ICancellationTokenSourceFactory with
        member x.CreateCancellationTokenSource() = async {
            let! dcts = DistributedCancellationToken.Create(source)
            return dcts :> ICloudCancellationTokenSource
        }
        
        member x.CreateLinkedCancellationTokenSource(parent: ICloudCancellationToken) = async {
            match parent with
            | :? DistributedCancellationToken as pdcts ->
                let! dcts = DistributedCancellationToken.Create(pdcts.Source, parent = pdcts)
                return dcts :> ICloudCancellationTokenSource        
            | _ -> return invalidArg "parent" "invalid cancellation token type."
        }