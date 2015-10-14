namespace MBrace.Core

open System.Diagnostics

/// Represents a distributed, atomically updatable value reference
type CloudAtom<'T> =
    inherit ICloudDisposable

    /// Cloud atom identifier
    abstract Id : string

    /// Cloud atom container Id
    abstract Container : string

    /// Gets the current value of the atom.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract Value : 'T

    /// Asynchronously gets the current value of the atom.
    abstract GetValueAsync : unit -> Async<'T>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="updater">Value transaction function.</param>
    /// <param name="maxRetries">Maximum retries under optimistic semantics. Defaults to infinite.</param>
    abstract TransactAsync : transaction:('T -> 'R * 'T) * ?maxRetries:int -> Async<'R>

    /// <summary>
    ///      Forces a value on atom.
    /// </summary>
    /// <param name="value">value to be set.</param>
    abstract ForceAsync : value:'T -> Async<unit>

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

open System.ComponentModel
open System.Runtime.CompilerServices

open MBrace.Core.Internals

#nowarn "444"

type CloudAtom =
    
    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    static member New<'T>(initial : 'T, ?atomId : string, ?container : string) : CloudLocal<CloudAtom<'T>> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        let container = defaultArg container provider.DefaultContainer
        let atomId = match atomId with None -> provider.GetRandomAtomIdentifier() | Some id -> id
        return! Cloud.OfAsync <| provider.CreateAtom(container, atomId, initial)
    }

    /// <summary>
    ///     Attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    static member GetById<'T>(atomId : string, ?container : string) : CloudLocal<CloudAtom<'T>> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        let container = defaultArg container provider.DefaultContainer
        return! Cloud.OfAsync <| provider.GetAtomById<'T>(container, atomId)
    }


    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    static member Delete (atom : CloudAtom<'T>) : CloudLocal<unit> = local {
        return! Cloud.OfAsync <| atom.Dispose()
    }


    /// <summary>
    ///     Deletes container and all its contained atoms.
    /// </summary>
    /// <param name="container">Container to be deleted.</param>
    static member DeleteContainer (container : string) : CloudLocal<unit> = local {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        return! Cloud.OfAsync <| provider.DisposeContainer container
    }

    /// <summary>
    ///     Increments a cloud counter by one.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    static member inline Increment (atom : CloudAtom<'T>) : CloudLocal<'T> = local {
        return! Cloud.OfAsync <| atom.TransactAsync (fun i -> let i' = i + LanguagePrimitives.GenericOne in i',i')
    }

    /// <summary>
    ///     Decrements a cloud counter by one.
    /// </summary>
    /// <param name="atom">Input atom.</param>
    static member inline Decrement (atom : CloudAtom<'T>) : CloudLocal<'T> = local {
        return! Cloud.OfAsync <| atom.TransactAsync (fun i -> let i' = i - LanguagePrimitives.GenericOne in i',i')
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


[<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
type CloudAtomExtensions =    

    /// Dereferences a cloud atom.
    [<Extension>]
    static member GetValue(this : CloudAtom<'T>) : CloudLocal<'T> = Cloud.OfAsync <| this.GetValueAsync()

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    [<Extension>]
    static member UpdateAsync (this : CloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int) : Async<unit> = async {
        return! this.TransactAsync((fun t -> (), updater t), ?maxRetries = maxRetries)
    }

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="transacter">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    [<Extension>]
    static member Transact (this : CloudAtom<'T>, transacter : 'T -> 'R * 'T, ?maxRetries : int) : CloudLocal<'R> = local {
        return! Cloud.OfAsync <| this.TransactAsync(transacter, ?maxRetries = maxRetries)
    }

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    [<Extension>]
    static member Update (this : CloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int) : CloudLocal<unit> = local {
        return! Cloud.OfAsync <| this.TransactAsync((fun t -> (), updater t), ?maxRetries = maxRetries)
    }

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    [<Extension>]
    static member Force (this : CloudAtom<'T>, value : 'T) : CloudLocal<unit> = local {
        return! Cloud.OfAsync <| this.ForceAsync value
    }