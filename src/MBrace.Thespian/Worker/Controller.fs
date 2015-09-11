namespace MBrace.Thespian.Runtime

open System

open Nessos.Thespian
open Nessos.Thespian.Remote

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

/// Defines actor logic used for managing cluster subscription state in a single MBrace worker.
[<AutoOpen>]
module internal WorkerController =

    /// Worker subscription state
    [<NoEquality; NoComparison>]
    type private WorkerState =
        | Idle
        /// Denotes a worker that hosts the cluster state state
        | MasterNode of ClusterState
        /// Denotes a worker that is subscribed to a remote cluster
        | SubscribedWorker of WorkerSubscription.Subscription

    /// Worker controller message API
    [<NoEquality; NoComparison>]
    type WorkerControllerMsg =
        /// Queries for the current worker state
        | GetState of IReplyChannel<(bool * ClusterState) option>
        /// Subscribe worker to provided state object
        | Subscribe of ClusterState * IReplyChannel<unit>
        /// Initializes worker as host of a new cluster state
        | InitMasterNode of ICloudFileStore * ResourceRegistry option * IReplyChannel<ClusterState>
        /// Resets state of worker instance
        | Reset of IReplyChannel<unit>
        /// Kill worker process with supplied error code
        | Kill of errorCode:int

    /// Controller actor name
    [<Literal>]
    let controllerActorName = "controller"

    /// Creates an MBrace uri from suppied controller ActorRef
    let mkUri (aref : ActorRef<WorkerControllerMsg>) =
        let uri = new Uri(ActorRef.toUri aref)
        sprintf "mbrace://%s:%d" uri.Host uri.Port

    /// Creates a controller actor ref from supplied MBrace uri
    let parseUri (uri : string) =
        let uri = new Uri(uri, UriKind.Absolute)
        if uri.Scheme <> "mbrace" then raise <| new FormatException("Invalid mbrace node uri.")
        let host = uri.Host
        let port = uri.Port
        let actorUri = sprintf "utcp://%s:%d/%s" host port controllerActorName
        ActorRef.fromUri<WorkerControllerMsg> actorUri

    /// <summary>
    ///     Initializes a controller actor with supplied initial parameters.
    /// </summary>
    /// <param name="useAppDomain">Use application domain isolation for MBrace work item execution.</param>
    /// <param name="maxConcurrentWorkItems">Maximum number of concurrent MBrace work items running in worker.</param>
    /// <param name="logger">Underlying system logger used by actor.</param>
    let initController (useAppDomain : bool) (maxConcurrentWorkItems : int) (logger : ISystemLogger) : ActorRef<WorkerControllerMsg> =
        let behaviour (state : WorkerState) (message : WorkerControllerMsg) = async {
            match message with
            | Kill i -> 
                logger.Log LogLevel.Critical "Received kill message, committing suicide in 2000ms."
                do! Async.Sleep 2000
                return exit i

            | GetState rc -> 
                match state with
                | Idle -> do! rc.Reply None
                | SubscribedWorker s -> do! rc.Reply (Some (false, s.RuntimeState))
                | MasterNode r -> do! rc.Reply (Some (true, r))

                return state

            | Subscribe (rs, rc) ->
                match state with
                | SubscribedWorker _
                | MasterNode _ ->
                    let e = new InvalidOperationException(sprintf "MBrace worker '%s' is already subscribed to a cluster." Config.LocalAddress)
                    do! rc.ReplyWithException e
                    return state
                | Idle ->
                    logger.Logf LogLevel.Info "Subscribing worker to runtime hosted at '%s'." rs.Uri
                    let! result = WorkerSubscription.initSubscription useAppDomain logger maxConcurrentWorkItems rs |> Async.Catch
                    match result with
                    | Choice1Of2 subscr -> 
                        do! rc.Reply (())
                        return SubscribedWorker subscr  
                    | Choice2Of2 e ->
                        logger.LogWithException LogLevel.Error e "Error subscribing worker node to cluster."
                        let e' = new Exception("Error subscribing worker node to cluster.", e)
                        do! rc.ReplyWithException e'
                        return state

            | Reset rc ->
                match state with
                | SubscribedWorker s -> 
                    s.Dispose()
                    do! rc.Reply (())
                    return Idle
                | Idle -> 
                    do! rc.Reply (())
                    return Idle

                | MasterNode _ ->
                    let e = new NotSupportedException(sprintf "Cannot reset worker acting as master node '%s'." Config.LocalAddress)
                    do! rc.ReplyWithException(e)
                    return state

            | InitMasterNode (store, resources, rc) ->
                match state with
                | Idle ->
                    logger.LogInfo "Initializing a new MBrace cluster hosted by this worker instance."
                    let result = 
                        try ClusterState.Create(store, isWorkerHosted = true, ?miscResources = resources) |> Choice1Of2
                        with e -> Choice2Of2 e

                    match result with
                    | Choice1Of2 rs ->
                        logger.Logf LogLevel.Info "Cluster '%O' has been initialized." rs.Id
                        do! rc.Reply rs
                        return MasterNode rs
                    | Choice2Of2 e ->
                        logger.LogWithException LogLevel.Error e "Error initializing hosted cluster."
                        let e' = new Exception("Error initializing hosted cluster.", e)
                        do! rc.ReplyWithException e'
                        return state

                | MasterNode _
                | SubscribedWorker _ ->
                    let e = new InvalidOperationException(sprintf "Cannot initialize cluster; worker is not idle.")
                    do! rc.ReplyWithException e
                    return state
        }

        let actor = Actor.Stateful Idle behaviour
        let aref = Actor.Publish(actor, name = controllerActorName) |> Actor.ref
        logger.Logf LogLevel.Info "Initialized controller actor:\n\t%O." aref
        aref