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

    let transact f =
        let result = ref Unchecked.defaultof<_>
        let f' t = let t',r = f t in result := r ; t'
        swap f' ; result.Value

    /// Get Current Value
    member __.Value with get() : 'T = !refCell

    /// <summary>
    /// Atomically updates the container.
    /// </summary>
    /// <param name="updateF">updater function.</param>
    member __.Swap (f : 'T -> 'T) : unit = swap f

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="transactionF">transaction function.</param>
    member __.Transact(f : 'T -> 'T * 'R) : 'R = transact f

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
    let swap (atom : Atom<'T>) f = atom.Swap f

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="atom">Atom to perform transaction on.</param>
    /// <param name="transactF">Transaction function.</param>
    let transact (atom : Atom<'T>) f : 'R = atom.Transact f

    /// <summary>
    ///     Force value on given atom.
    /// </summary>
    /// <param name="atom">Atom to be updated.</param>
    /// <param name="value">Value to be set.</param>
    let force (atom : Atom<'T>) t = atom.Force t


/// thread safe cache with expiry semantics
[<AutoSerializable(false)>]
type CacheAtom<'T> internal (provider : 'T option -> 'T, interval : TimeSpan, initial : 'T option) =
    let initial = match initial with None -> Undefined | Some t -> Success(t, DateTime.Now)
    let container = Atom.create<CacheState<'T>> initial

    let update force state =
        let inline compute lastSuccessful =
            try Success(provider lastSuccessful, DateTime.Now)
            with e -> Error(e, DateTime.Now, lastSuccessful)

        let state' =
            match state with
            | Undefined -> compute None
            | Success (_,time) when not force && DateTime.Now - time <= interval -> state
            | Error (_,time,_) when not force && DateTime.Now - time <= interval -> state
            | Success (t,_) -> compute <| Some t
            | Error (_,_,last) -> compute last

        match state' with
        | Success(t,_) -> state', Choice1Of2 t
        | Error(e,_,_) -> state', Choice2Of2 e
        | _ -> failwith "impossible"

    member __.Value =
        match container.Transact (update false) with
        | Choice1Of2 v -> v
        | Choice2Of2 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e

    member __.TryGetValue () =
        match container.Transact (update false) with
        | Choice1Of2 v -> Some v
        | Choice2Of2 _ -> None

    member __.TryGetLastSuccessfulValue () =
        match __.TryGetValue () with
        | Some _ as v -> v
        | None ->
            // failed, try extracting the latest recorded value
            match container.Value with
            | Success(t,_)
            | Error(_,_,Some t) -> Some t
            | _ -> None

    member __.Force () =
        match container.Transact (update true) with
        | Choice1Of2 v -> v
        | Choice2Of2 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e

and private CacheState<'T> =
    | Undefined
    | Error of exn * DateTime * 'T option // last successful state
    | Success of 'T * DateTime

/// thread safe cache with expiry semantics
and CacheAtom =

    /// <summary>
    ///     Creates a new CacheAtom instance.
    /// </summary>
    /// <param name="provider">Provider function for renewing the value.</param>
    /// <param name="keepLastResultOnError">Return previous result on exception or throw. Defaults to the latter.</param>
    /// <param name="intervalMilliseconds">Value renew threshold in milliseconds. Defaults to 200.</param>
    /// <param name="initial">Initial value for cell. Defaults to no value.</param>
    static member Create(provider : unit -> 'T, ?keepLastResultOnError, ?intervalMilliseconds, ?initial) : CacheAtom<'T> =
        let keepLastResultOnError = defaultArg keepLastResultOnError false
        let interval = defaultArg intervalMilliseconds 200 |> float |> TimeSpan.FromMilliseconds
        let providerW (state : 'T option) =
            try provider ()
            with e ->
                match state with
                | Some t when keepLastResultOnError -> t
                | _ -> reraise ()
            
        new CacheAtom<'T>(providerW, interval, initial)

    /// <summary>
    ///     Creates a new CacheAtom instance.
    /// </summary>
    /// <param name="init">Initial value.</param>
    /// <param name="updater">Updater function.</param>
    /// <param name="intervalMilliseconds">Value renew threshold in milliseconds. Defaults to 200.</param>
    /// <param name="keepLastResultOnError">Return previous result on exception or throw. Defaults to the latter.</param>
    static member Create(init : 'T, f : 'T -> 'T, ?intervalMilliseconds, ?keepLastResultOnError) : CacheAtom<'T> =
        let keepLastResultOnError = defaultArg keepLastResultOnError false
        let interval = defaultArg intervalMilliseconds 200 |> float |> TimeSpan.FromMilliseconds
        let providerW (state : 'T option) =
            try f state.Value
            with e ->
                match state with
                | Some t when keepLastResultOnError -> t
                | _ -> reraise ()

        new CacheAtom<'T>(providerW, interval, Some init)
