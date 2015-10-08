namespace MBrace.Core

/// Represents a distributed, atomically updatable value reference
type CloudAtom<'T> =
    inherit ICloudDisposable

    /// Cloud atom identifier
    abstract Id : string

    /// Cloud atom container Id
    abstract Container : string

    /// Gets the current value of the atom.
    abstract Value : 'T

    /// Asynchronously gets the current value of the atom.
    abstract GetValueAsync : unit -> Async<'T>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="updater">Value transaction function.</param>
    /// <param name="maxRetries">Maximum retries under optimistic semantics. Defaults to infinite.</param>
    abstract Transact : transaction:('T -> 'R * 'T) * ?maxRetries:int -> Async<'R>

    /// <summary>
    ///      Forces a value on atom.
    /// </summary>
    /// <param name="value">value to be set.</param>
    abstract Force : value:'T -> Async<unit>

namespace MBrace.Core.Internals
 
open MBrace.Core

/// Defines a factory for distributed atoms
type ICloudAtomProvider =

    /// Implementation name
    abstract Name : string

    /// Cloud atom identifier
    abstract Id : string

    /// Create a uniquely specified atom container name.
    abstract GetRandomContainerName : unit -> string

    /// Create a uniquely specified atom identifier.
    abstract GetRandomAtomIdentifier : unit -> string

    /// Gets the default container used by the atom provider
    abstract DefaultContainer : string

    /// <summary>
    ///     Creates a copy of the atom provider with updated default container.
    /// </summary>
    /// <param name="container">Container to be updated.</param>
    abstract WithDefaultContainer : container:string -> ICloudAtomProvider

    /// <summary>
    ///     Checks if provided value is supported in atom instances.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSupportedValue : value:'T -> bool

    /// <summary>
    ///     Creates a new atom instance with given initial value.
    /// </summary>
    /// <param name="container">CloudAtom container identifier.</param>
    /// <param name="atomId">CloudAtom unique identifier.</param>
    /// <param name="initValue">Initial value to populate atom with.</param>
    abstract CreateAtom<'T> : container:string * atomId: string * initValue:'T -> Async<CloudAtom<'T>>

    /// <summary>
    ///     Gets an already existing CloudAtom of specified identifiers and type.
    /// </summary>
    /// <param name="container">CloudAtom container identifier.</param>
    /// <param name="atomId">CloudAtom unique identifier.</param>
    abstract GetAtomById<'T> : container:string * atomId:string -> Async<CloudAtom<'T>>

    /// <summary>
    ///     Disposes all atoms in provided container
    /// </summary>
    /// <param name="container">Atom container.</param>
    abstract DisposeContainer : container:string -> Async<unit>


namespace MBrace.Core

open MBrace.Core.Internals

#nowarn "444"

type CloudAtom =
    
    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    static member New<'T>(initial : 'T, ?atomId : string, ?container : string) : Local<CloudAtom<'T>> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        let container = defaultArg container provider.DefaultContainer
        let atomId = match atomId with None -> provider.GetRandomAtomIdentifier() | Some id -> id
        return! provider.CreateAtom(container, atomId, initial)
    }

    /// <summary>
    ///     Attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    static member GetById<'T>(atomId : string, ?container : string) : Local<CloudAtom<'T>> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        let container = defaultArg container provider.DefaultContainer
        return! provider.GetAtomById<'T>(container, atomId)
    }

    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    static member Read(atom : CloudAtom<'T>) : Local<'T> = local {
        return! atom.GetValueAsync()
    }

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    static member Update (atom : CloudAtom<'T>, updateF : 'T -> 'T, ?maxRetries : int)  : Local<unit> = local {
        return! atom.Transact((fun t -> (), updateF t), ?maxRetries = maxRetries)
    }

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    static member Force (atom : CloudAtom<'T>, value : 'T) : Local<unit> = local {
        return! atom.Force value
    }

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    /// <param name="transactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    static member Transact (atom : CloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : Local<'R> = local {
        return! atom.Transact(transactF, ?maxRetries = maxRetries)
    }

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    static member Delete (atom : CloudAtom<'T>) : Local<unit> = local {
        return! atom.Dispose()
    }

    /// <summary>
    ///     Deletes container and all its contained atoms.
    /// </summary>
    /// <param name="container"></param>
    static member DeleteContainer (container : string) : Local<unit> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        return! provider.DisposeContainer container
    }

    /// Generates a unique container name.
    static member CreateRandomContainerName() = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        return provider.GetRandomContainerName()
    }

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    static member IsSupportedValue(value : 'T) = local {
        let! provider = Cloud.TryGetResource<ICloudAtomProvider> ()
        return
            match provider with
            | None -> false
            | Some ap -> ap.IsSupportedValue value
    }

    /// <summary>
    ///     Increments a cloud counter by one.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    static member inline Increment (atom : CloudAtom<'T>) : Local<'T> = local {
        return! atom.Transact (fun i -> let i' = i + LanguagePrimitives.GenericOne in i',i')
    }

    /// <summary>
    ///     Decrements a cloud counter by one.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    static member inline Decrement (atom : CloudAtom<'T>) : Local<'T> = local {
        return! atom.Transact (fun i -> let i' = i - LanguagePrimitives.GenericOne in i',i')
    }
