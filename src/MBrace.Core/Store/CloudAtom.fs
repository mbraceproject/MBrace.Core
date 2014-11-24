namespace Nessos.MBrace.Store

open System
open System.IO
open System.Runtime.Serialization

open Nessos.MBrace
open Nessos.MBrace.Runtime

/// Defines a distributed key/value storage service abstraction
type ICloudAtomProvider =
    inherit IResource

    abstract IsSupportedValue : 'T -> bool

    /// <summary>
    ///     Checks if entry with provided key exists.
    /// </summary>
    /// <param name="id">Entry id.</param>
    abstract Exists : id:string -> Async<bool>

    /// <summary>
    ///     Deletes entry with provided key.
    /// </summary>
    /// <param name="id"></param>
    abstract Delete : id:string -> Async<unit>

    /// <summary>
    ///     Creates a new entry with provided initial value.
    ///     Returns the key identifier for new entry.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    abstract Create<'T> : initial:'T -> Async<string>

    /// <summary>
    ///     Returns the current value of provided atom.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    abstract GetValue<'T> : id:string -> Async<'T>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    /// <param name="updater">Updating function.</param>
    abstract Update : id:string * updater:('T -> 'T) -> Async<unit>

    /// <summary>
    ///     Force update of existing table entry with given value.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    /// <param name="value">Value to be set.</param>
    abstract Force : id:string * value:'T -> Async<unit>

    /// <summary>
    ///     Enumerates all table entries.
    /// </summary>
    abstract Enumerate : unit -> Async<(string * Type) []>

/// Represent a distributed atomically updatable value container
[<Sealed; AutoSerializable(true)>]
type CloudAtom<'T> =

    [<NonSerialized>]
    val mutable private provider : ICloudAtomProvider
    val private providerId : string
    val private id : string

    internal new (provider : ICloudAtomProvider, id : string) =
        {
            provider = provider
            providerId = Dependency.GetId<ICloudAtomProvider> provider
            id = id
        }

    [<OnDeserializedAttribute>]
    member private __.OnDeserialized(_ : StreamingContext) =
        __.provider <- Dependency.Resolve<ICloudAtomProvider> __.providerId
    
    /// Atom identifier
    member __.Id = __.id

    /// <summary>
    ///     Atomically updates the contained value.  
    /// </summary>
    /// <param name="updater">value updating function.</param>
    member __.Update(updater : 'T -> 'T) : Async<unit> = __.provider.Update(__.id, updater)

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
    member __.Force(value : 'T) = __.provider.Force(__.id, value)
    
    /// <summary>
    ///     Asynchronously returns the contained value for given atom.
    /// </summary>
    member __.GetValue () = __.provider.GetValue<'T> __.id

    /// Synchronously returns the contained value for given atom.
    member __.Value = Async.RunSync (__.provider.GetValue<'T> __.id)

    interface ICloudDisposable with
        member __.Dispose () = __.provider.Delete __.id
    
[<AutoOpen>]
module CloudAtomUtils =

    type ICloudAtomProvider with
        /// <summary>
        ///     Creates a new atom instance.
        /// </summary>
        /// <param name="init">Initial value.s</param>
        member p.CreateAtom<'T> (init : 'T) = async {
            let! id = p.Create<'T>(init)
            return new CloudAtom<'T>(p, id)
        }

        /// <summary>
        ///     Deletes provided cloud atom instance.
        /// </summary>
        /// <param name="atom">Atom to be deleted.</param>
        member p.Delete (atom : CloudAtom<'T>) = p.Delete atom.Id