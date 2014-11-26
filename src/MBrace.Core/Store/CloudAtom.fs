namespace Nessos.MBrace

open System
open System.IO
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

/// Represent a distributed atomically updatable value container
[<Sealed; AutoSerializable(true)>]
type CloudAtom<'T> internal (store : ICloudStore, id : string) =

    [<NonSerialized>]
    let mutable tableStore = store.TableStore
    let storeId = store.UUID
    // delayed store bootstrapping after deserialization
    let getStore() =
        match tableStore with
        | Some ts -> ts
        | None ->
            let ts = CloudStoreRegistry.Resolve(id).TableStore |> Option.get
            tableStore <- Some ts
            ts
    
    /// Atom identifier
    member __.Id = id

    /// <summary>
    ///     Atomically updates the contained value.  
    /// </summary>
    /// <param name="updater">value updating function.</param>
    member __.Update(updater : 'T -> 'T) : Async<unit> = getStore().Update(id, updater)

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="transaction">Transaction function.</param>
    member __.Transact(transaction : 'T -> 'R * 'T) : Async<'R> = async {
        let result = ref Unchecked.defaultof<'R>
        do! __.Update(fun t -> let r,t' = transaction t in result := r ; t')
        return result.Value
    }

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be forced.</param>
    member __.Force(value : 'T) =  getStore().Force(id, value)
    
    /// <summary>
    ///     Asynchronously returns the contained value for given atom.
    /// </summary>
    member __.GetValue () = getStore().GetValue<'T> id

    /// Synchronously returns the contained value for given atom.
    member __.Value = Async.RunSync (getStore().GetValue<'T> id)

    interface ICloudDisposable with
        member __.Dispose () = getStore().Delete id

    static member internal Create(value : 'T, store : ICloudStore) = async {
        match store.TableStore with
        | None -> return raise <| ResourceNotFoundException "Table store not available in this runtime."
        | Some ts ->
            let! id = ts.Create<'T>(value)
            return new CloudAtom<'T>(store, id)
    }


namespace Nessos.MBrace.Store

open Nessos.MBrace
open Nessos.MBrace.Runtime
    
[<AutoOpen>]
module CloudAtomUtils =

    type ICloudStore with
        /// <summary>
        ///     Creates a new atom instance.
        /// </summary>
        /// <param name="init">Initial value.s</param>
        member s.CreateAtom<'T> (init : 'T) = CloudAtom<'T>.Create(init, s)