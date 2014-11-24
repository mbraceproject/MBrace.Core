[<AutoOpen>]
module Nessos.MBrace.StoreCombinators

open System
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

#nowarn "444"

/// Represents a file stored in the cloud storage service.
type CloudFile = Nessos.MBrace.Store.CloudFile

type Nessos.MBrace.Store.CloudFile with

    /// <summary> 
    ///     Create a new file in the storage with the specified folder and name.
    ///     Use the serialize function to write to the underlying stream.
    /// </summary>
    /// <param name="serializer">Function that will write data on the underlying stream.</param>
    /// <param name="uri">Target uri for given cloud file. Defaults to runtime-assigned path.</param>
    static member Create(serializer : Stream -> Async<unit>, ?path : string) : Cloud<CloudFile> = cloud {
        let! storeConfig = Cloud.GetResource<CloudStoreConfiguration> ()
        let path = match path with Some p -> p | None -> storeConfig.FileProvider.CreateUniqueFileName storeConfig.DefaultContainer
        return! Cloud.OfAsync <| storeConfig.FileProvider.CreateFile(path, serializer)
    }

    /// <summary>
    ///     Returns an existing cloud file instance from provided path.
    /// </summary>
    /// <param name="path">Input path to cloud file.</param>
    static member FromPath(path : string) = cloud {
        let! provider = Cloud.GetResource<ICloudFileProvider> ()
        return! Cloud.OfAsync <| provider.FromPath(path)
    }

    /// <summary> 
    ///     Read the contents of a CloudFile using the given deserialize/reader function.
    /// </summary>
    /// <param name="cloudFile">CloudFile to read.</param>
    /// <param name="deserializer">Function that reads data from the underlying stream.</param>
    static member Read(cloudFile : CloudFile, deserializer : Stream -> Async<'T>) : Cloud<'T> =
        Cloud.OfAsync <| cloudFile.Read deserializer

    /// <summary> 
    ///     Returns all CloudFiles in given container.
    /// </summary>
    /// <param name="container">The container (folder) to search.</param>
    static member Enumerate(container : string) : Cloud<CloudFile []> = cloud {
        let! provider = Cloud.GetResource<ICloudFileProvider> ()
        return! Cloud.OfAsync <| provider.EnumerateCloudFiles container
    }

/// Represent a distributed atomically updatable value container
type CloudAtom<'T> = Nessos.MBrace.Store.CloudAtom<'T>

/// CloudAtom utility functions
type CloudAtom =
    
    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    static member Create<'T>(initial : 'T) = cloud {
        let! provider = Cloud.GetResource<ICloudAtomProvider> ()
        return! Cloud.OfAsync <| provider.CreateAtom initial
    }

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    static member Update (updateF : 'T -> 'T) (atom : CloudAtom<'T>) = 
        Cloud.OfAsync <| atom.Update updateF

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    static member Force (value : 'T) (atom : CloudAtom<'T>) =
        Cloud.OfAsync <| atom.Force value

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="trasactF"></param>
    /// <param name="atom"></param>
    static member Transact (trasactF : 'T -> 'R * 'T) (atom : CloudAtom<'T>) =
        Cloud.OfAsync <| atom.Transact trasactF

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    static member Delete (atom : CloudAtom<'T>) = Cloud.Dispose atom

///// Represents an immutable reference to an
///// object that is persisted in the underlying store.
///// Cloud references are cached locally for performance.
//type CloudRef<'T> = Nessos.MBrace.Store.CloudRef<'T>
//

//
///// Cloud sequence methods.
//type CloudSeq =
//
//    /// <summary>
//    ///     Creates a new cloud sequence with given values in the underlying store.
//    ///     Cloud sequences are cached locally for performance.
//    /// </summary>
//    /// <param name="values">Collection to populate the cloud sequence with.</param>
//    static member New(values : seq<'T>) = cloud {
//        let! storeP = Cloud.GetResource<StoreProvider> ()
//        return! Cloud.OfAsync <| CloudSeq<'T>.Create(values, storeP)
//    }