namespace MBrace.Runtime.Utils

open System
open System.Threading

open MBrace.Core.Internals

/// Thread-safe value container with optimistic update semantics
type Atom<'T when 'T : not struct>(value : 'T) =
    let refCell = ref value

    let rec swap f = 
        let currentValue = !refCell
        let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
        if obj.ReferenceEquals(result, currentValue) then ()
        else Thread.SpinWait 20; swap f

    let rec swapAsync (f : 'T -> Async<'T>) = async {
        let currentValue = !refCell
        let! newValue = f currentValue
        let result = Interlocked.CompareExchange<'T>(refCell, newValue, currentValue)
        if obj.ReferenceEquals(result, currentValue) then ()
        else Thread.SpinWait 20; do! swapAsync f
    }

    /// Get Current Value
    member __.Value with get() : 'T = !refCell

    /// <summary>
    /// Atomically updates the container.
    /// </summary>
    /// <param name="updater">updater function.</param>
    member __.Swap (updater : 'T -> 'T) : unit = swap updater

    /// <summary>
    /// Asynchronously updates the container.
    /// </summary>
    /// <param name="updater">Asynchronous updater function.</param>
    member __.SwapAsync (updater : 'T -> Async<'T>) : Async<unit> = swapAsync updater

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="transactionF">transaction function.</param>
    member __.Transact(transactionF : 'T -> 'T * 'R) : 'R =
        let result = ref Unchecked.defaultof<'R>
        let f t = let t',r = transactionF t in result := r ; t'
        swap f ; result.Value

    /// <summary>
    ///     Asynchronously transacts container.
    /// </summary>
    /// <param name="transactionF">Asynchronous transaction function.</param>
    member __.TransactAsync(transactionF : 'T -> Async<'T * 'R>) : Async<'R> = async {
        let result = ref Unchecked.defaultof<'R>
        let f t = async { let! t',r = transactionF t in return (result := r ; t') }
        do! swapAsync f
        return result.Value
    }


    /// <summary>
    /// Force a new value on container.
    /// </summary>
    /// <param name="value">value to be set.</param>
    member __.Force (value : 'T) = refCell := value

/// Atom utilities module
[<RequireQualifiedAccess>]
module Atom =

    /// <summary>
    /// Initialize a new atomic container with given value.
    /// </summary>
    /// <param name="value">Initial value.</param>
    let create<'T when 'T : not struct> value = new Atom<'T>(value)
    
    /// <summary>
    /// Atomically updates the container with given function.
    /// </summary>
    /// <param name="atom">Atom to be updated.</param>
    /// <param name="updateF">Updater function.</param>
    let swap (atom : Atom<'T>) (f : 'T -> 'T) = atom.Swap f

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="atom">Atom to perform transaction on.</param>
    /// <param name="transactF">Transaction function.</param>
    let transact (atom : Atom<'T>) (f : 'T -> 'T * 'R) : 'R = atom.Transact f

    /// <summary>
    ///     Force value on given atom.
    /// </summary>
    /// <param name="atom">Atom to be updated.</param>
    /// <param name="value">Value to be set.</param>
    let force (atom : Atom<'T>) t = atom.Force t


[<AutoSerializable(false); NoEquality; NoComparison>]
type private CacheState<'T> =
    | Undefined
    | Error of exn * DateTime * 'T option // last successful state
    | Success of 'T * DateTime

/// thread safe cache with expiry semantics
[<AutoSerializable(false)>]
type CacheAtom<'T> internal (provider : 'T option -> Async<'T>, interval : TimeSpan, initial : 'T option) =
    let initial = match initial with None -> Undefined | Some t -> Success(t, DateTime.Now)
    let container = Atom.create<CacheState<'T>> initial

    let update force state = async {
        let compute lastSuccessful = async {
            let! result = Async.Catch(provider lastSuccessful)
            return
                match result with
                | Choice1Of2 t -> Success(t, DateTime.Now)
                | Choice2Of2 e -> Error(e, DateTime.Now, lastSuccessful)
        }

        let! state' = async {
            match state with
            | Undefined -> return! compute None
            | Success (_,time) when not force && DateTime.Now - time <= interval -> return state
            | Error (_,time,_) when not force && DateTime.Now - time <= interval -> return state
            | Success (t,_) -> return! compute <| Some t
            | Error (_,_,last) -> return! compute last
        }

        return
            match state' with
            | Success(t,_) -> state', Choice1Of2 t
            | Error(e,_,_) -> state', Choice2Of2 e
            | _ -> failwith "impossible"
    }

    /// Asynchronously returns the latest cached value
    member __.GetValueAsync() = async {
        let! result = container.TransactAsync (update false)
        return
            match result with
            | Choice1Of2 v -> v
            | Choice2Of2 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e
    }

    /// Asynchronously returns the latest cached value
    member __.TryGetValueAsync () = async {
        let! result = container.TransactAsync (update false)
        return
            match result with
            | Choice1Of2 v -> Some v
            | Choice2Of2 _ -> None
    }

    /// Asynchronously returns the latest successful value
    member __.TryGetLastSuccessfulValueAsync () = async {
        let! result = __.TryGetValueAsync()
        return
            match result with
            | Some _ as v -> v
            | None ->
                // failed, try extracting the latest recorded value
                match container.Value with
                | Success(t,_)
                | Error(_,_,Some t) -> Some t
                | _ -> None
    }

    /// Forces update of a new value
    member __.ForceAsync () = async {
        let! result = container.TransactAsync (update true)
        return
            match result with
            | Choice1Of2 v -> v
            | Choice2Of2 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e
    }

    member __.Value = __.GetValueAsync() |> Async.RunSync
    member __.TryGetValue () = __.TryGetValueAsync() |> Async.RunSync
    member __.TryGetLastSuccessfulValue () = __.TryGetLastSuccessfulValueAsync() |> Async.RunSync
    member __.Force () = __.ForceAsync() |> Async.RunSync

/// thread safe cache with expiry semantics
type CacheAtom =

    /// <summary>
    ///     Creates a new CacheAtom instance.
    /// </summary>
    /// <param name="provider">Provider function for renewing the value.</param>
    /// <param name="keepLastResultOnError">Return previous result on exception or throw. Defaults to the latter.</param>
    /// <param name="intervalMilliseconds">Value renew threshold in milliseconds. Defaults to 200.</param>
    /// <param name="initial">Initial value for cell. Defaults to no value.</param>
    static member Create(provider : Async<'T>, ?keepLastResultOnError, ?intervalMilliseconds, ?initial) : CacheAtom<'T> =
        let keepLastResultOnError = defaultArg keepLastResultOnError false
        let interval = defaultArg intervalMilliseconds 200 |> float |> TimeSpan.FromMilliseconds
        let providerW (state : 'T option) = async {
            try return! provider
            with e ->
                match state with
                | Some t when keepLastResultOnError -> return t
                | _ -> return! Async.Raise e
        }
            
        new CacheAtom<'T>(providerW, interval, initial)

    /// <summary>
    ///     Creates a new CacheAtom instance.
    /// </summary>
    /// <param name="init">Initial value.</param>
    /// <param name="updater">Updater function.</param>
    /// <param name="intervalMilliseconds">Value renew threshold in milliseconds. Defaults to 200.</param>
    /// <param name="keepLastResultOnError">Return previous result on exception or throw. Defaults to the latter.</param>
    static member Create(init : 'T, f : 'T -> Async<'T>, ?intervalMilliseconds, ?keepLastResultOnError) : CacheAtom<'T> =
        let keepLastResultOnError = defaultArg keepLastResultOnError false
        let interval = defaultArg intervalMilliseconds 200 |> float |> TimeSpan.FromMilliseconds
        let providerW (state : 'T option) = async {
            try return! f state.Value
            with e ->
                match state with
                | Some t when keepLastResultOnError -> return t
                | _ -> return! Async.Raise e
        }

        new CacheAtom<'T>(providerW, interval, Some init)