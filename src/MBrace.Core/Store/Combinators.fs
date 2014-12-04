[<AutoOpen>]
module Nessos.MBrace.StoreCombinators

open System
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

#nowarn "444"

//type CloudStore =
//    
//    /// Returns the default container for current execution context.
//    static member GetDefaultContainer () = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        return config.DefaultFileContainer
//    }
//
//    /// Enumerates all containers in file store in execution context.
//    static member EnumerateContainers () = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        return config.FileStore.EnumerateContainers()
//    }
//
//    /// Generates a unique container name for current execution context.
//    static member GetUniqueContainerName () = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        return config.FileStore.CreateUniqueContainerName()
//    }
//
//    /// Generates a unique container name for current execution context.
//    static member GetUniqueFileName (?container : string) = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let container = match container with Some c -> c | None -> config.DefaultFileContainer
//        return config.FileStore.CreateUniqueFileName(container)
//    }
//
//    /// Creates an absolute path for a sequence of filenames with given container
//    static member GetFullPath (files : seq<string>, ?container : string) = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let container = match container with Some c -> c | None -> config.DefaultFileContainer
//        return files |> Seq.map (fun f -> config.FileStore.Combine(container, f)) |> Seq.toArray
//    }

//type Nessos.MBrace.CloudFile with
//
//    /// <summary> 
//    ///     Create a new file in the storage with the specified folder and name.
//    ///     Use the serialize function to write to the underlying stream.
//    /// </summary>
//    /// <param name="serializer">Function that will write data on the underlying stream.</param>
//    /// <param name="path">Target uri for given cloud file. Defaults to runtime-assigned path.</param>
//    static member New(serializer : Stream -> Async<unit>, ?path : string) : Cloud<CloudFile> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let path = match path with Some p -> p | None -> config.FileStore.CreateUniqueFileName config.DefaultFileContainer
//        return! Cloud.OfAsync <| config.FileStore.CreateFile(path, serializer)
//    }
//
//    /// <summary>
//    ///     Returns an existing cloud file instance from provided path.
//    /// </summary>
//    /// <param name="path">Input path to cloud file.</param>
//    static member FromPath(path : string) : Cloud<CloudFile> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        return! Cloud.OfAsync <| config.FileStore.FromPath(path)
//    }
//
//    /// <summary> 
//    ///     Read the contents of a CloudFile using the given deserialize/reader function.
//    /// </summary>
//    /// <param name="cloudFile">CloudFile to read.</param>
//    /// <param name="deserializer">Function that reads data from the underlying stream.</param>
//    static member Read(cloudFile : CloudFile, deserializer : Stream -> Async<'T>) : Cloud<'T> =
//        Cloud.OfAsync <| cloudFile.Read deserializer
//
//    /// <summary> 
//    ///     Returns all CloudFiles in given container.
//    /// </summary>
//    /// <param name="container">The container (folder) to search. Defaults to context container.</param>
//    static member EnumerateFiles(?container : string) : Cloud<CloudFile []> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let container = match container with Some c -> c | None -> config.DefaultFileContainer
//        return! Cloud.OfAsync <| config.FileStore.EnumerateCloudFiles container
//    }

/// CloudAtom utility functions
//type CloudAtom =
//    
//    /// <summary>
//    ///     Creates a new cloud atom instance with given value.
//    /// </summary>
//    /// <param name="initial">Initial value.</param>
//    static member New<'T>(initial : 'T) : Cloud<CloudAtom<'T>> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        match config.TableStore with
//        | None -> 
//            let msg = sprintf "Table storage not available in current execution context."
//            return raise <| ResourceNotFoundException msg
//        | Some ts ->
//            return! Cloud.OfAsync <| ts.CreateAtom initial
//    }
//
//    /// <summary>
//    ///     Dereferences a cloud atom.
//    /// </summary>
//    /// <param name="atom">Atom instance.</param>
//    static member Read(atom : CloudAtom<'T>) : Cloud<'T> = Cloud.OfAsync <| atom.GetValue()
//
//    /// <summary>
//    ///     Atomically updates the contained value.
//    /// </summary>
//    /// <param name="updater">value updating function.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    static member Update (updateF : 'T -> 'T) (atom : CloudAtom<'T>) : Cloud<unit> = 
//        Cloud.OfAsync <| atom.Update updateF
//
//    /// <summary>
//    ///     Forces the contained value to provided argument.
//    /// </summary>
//    /// <param name="value">Value to be set.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    static member Force (value : 'T) (atom : CloudAtom<'T>) : Cloud<unit> =
//        Cloud.OfAsync <| atom.Force value
//
//    /// <summary>
//    ///     Transactionally updates the contained value.
//    /// </summary>
//    /// <param name="trasactF"></param>
//    /// <param name="atom"></param>
//    static member Transact (trasactF : 'T -> 'R * 'T) (atom : CloudAtom<'T>) : Cloud<'R> =
//        Cloud.OfAsync <| atom.Transact trasactF
//
//    /// <summary>
//    ///     Deletes the provided atom instance from store.
//    /// </summary>
//    /// <param name="atom">Atom instance to be deleted.</param>
//    static member Delete (atom : CloudAtom<'T>) = Cloud.Dispose atom
//
//
//    /// <summary>
//    ///     Checks if value is supported by current table store.
//    /// </summary>
//    /// <param name="value">Value to be checked.</param>
//    static member IsSupportedValue(value : 'T) = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        return
//            match config.TableStore with
//            | None -> false
//            | Some ts -> ts.IsSupportedValue value
//    }


///// Cloud reference methods.
//type CloudRef =
//
//    /// <summary>
//    ///     Creates a new cloud reference to the underlying store with provided value.
//    ///     Cloud references are immutable and cached locally for performance.
//    /// </summary>
//    /// <param name="value">Cloud reference value.</param>
//    /// <param name="container">FileStore container used for cloud req. Defaults to process default.</param>
//    /// <param name="serializer">Serialization used for object serialization. Default to runtime default.</param>
//    static member New(value : 'T, ?container : string, ?serializer : ISerializer) : Cloud<CloudRef<'T>> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let container = defaultArg container config.DefaultFileContainer
//        let serializer = defaultArg serializer config.Serializer
//        return! Cloud.OfAsync <| config.FileStore.CreateCloudRef(value, container, serializer)
//    }
//
//    /// <summary>
//    ///     Dereference a Cloud reference.
//    /// </summary>
//    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
//    static member Read(cloudRef : CloudRef<'T>) : Cloud<'T> = Cloud.OfAsync <| cloudRef.GetValue()
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
//    /// <param name="container">FileStore container used for cloud req. Defaults to process default.</param>
//    /// <param name="serializer">Serialization used for object serialization. Default to runtime default.</param>
//    static member New(values : seq<'T>, ?container, ?serializer) : Cloud<CloudSeq<'T>> = cloud {
//        let! config = Cloud.GetResource<CloudStoreConfiguration> ()
//        let container = defaultArg container config.DefaultFileContainer
//        let serializer = defaultArg serializer config.Serializer
//        return! Cloud.OfAsync <| config.FileStore.CreateCloudSeq<'T>(values, container, serializer)
//    }