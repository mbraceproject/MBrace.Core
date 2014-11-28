namespace Nessos.MBrace

open System
open System.IO
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

/// Represent a distributed atomically updatable value container
[<Sealed; AutoSerializable(true)>]
type CloudAtom<'T> internal (tableStore : ICloudTableStore, id : string) =

    let storeId = tableStore.UUID
    [<NonSerialized>]
    let mutable tableStore = Some tableStore
    // delayed store bootstrapping after deserialization
    let getStore() =
        match tableStore with
        | Some ts -> ts
        | None ->
            let ts = StoreRegistry.GetTableStore storeId
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


namespace Nessos.MBrace.Store

open Nessos.MBrace
    
[<AutoOpen>]
module CloudAtomUtils =

    type ICloudTableStore with
        /// <summary>
        ///     Creates a new atom instance.
        /// </summary>
        /// <param name="init">Initial value.s</param>
        member ts.CreateAtom<'T> (init : 'T) = async {
            let! id = ts.Create<'T>(init)
            return new CloudAtom<'T>(ts, id)
        }