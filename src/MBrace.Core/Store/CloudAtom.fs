namespace MBrace.Core

/// Represents a distributed, atomically updatable value reference
type CloudAtom<'T> =
    inherit ICloudDisposable

    /// Cloud atom identifier
    abstract Id : string

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

    /// Create a uniquely specified container name.
    abstract CreateUniqueContainerName : unit -> string

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
    /// <param name="container">Atom container.</param>
    /// <param name="initValue"></param>
    abstract CreateAtom<'T> : container:string * initValue:'T -> Async<CloudAtom<'T>>

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
    static member New<'T>(initial : 'T, ?container : string) : Local<CloudAtom<'T>> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        let container = defaultArg container provider.DefaultContainer
        return! provider.CreateAtom(container, initial)
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
    static member CreateContainerName() = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        return provider.CreateUniqueContainerName()
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
    static member inline Incr (atom : CloudAtom<'T>) : Local<'T> = local {
        return! atom.Transact (fun i -> let i' = i + LanguagePrimitives.GenericOne in i',i')
    }

    /// <summary>
    ///     Decrements a cloud counter by one.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    static member inline Decr (atom : CloudAtom<'T>) : Local<'T> = local {
        return! atom.Transact (fun i -> let i' = i - LanguagePrimitives.GenericOne in i',i')
    }
