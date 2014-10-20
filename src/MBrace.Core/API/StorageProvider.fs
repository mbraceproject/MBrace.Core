namespace Nessos.MBrace.Runtime

open Nessos.MBrace

/// <summary>
///     Abstract storage provider resource.
/// </summary>
type IStorageProvider =

    /// <summary>
    ///     Returns a unique, randomized container (directory) name 
    ///     valid in the underlying storage implementation.
    /// </summary>
    abstract GetRandomContainer : unit -> string

    /// <summary>
    ///     Returns a unique, randomized uri (file) name 
    ///     valid in the underlying storage implementation.
    /// </summary>
    /// <param name="container">Container for the file name. Defaults to process container.</param>
    abstract GetRandomUri : ?container:string -> string

    /// <summary>
    ///     Checks if provided uri is valid for given storage implementation.
    /// </summary>
    /// <param name="uri">Uri to be examined.</param>
    abstract IsValidUri : uri:string -> bool

    /// <summary>
    ///     Returns container (directory) name for a valid uri in current storage implementation.
    /// </summary>
    /// <param name="uri">Uri to be parsed.</param>
    abstract GetContainerName : uri:string -> string

    /// <summary>
    ///     Returns the file name for a valid uri in current storage implementation.
    /// </summary>
    /// <param name="uri">Uri to be parsed.</param>
    abstract GetFileName : uri:string -> string


    /// <summary>
    ///     Returns all containers (directories) found in the underlying storage implementation.
    /// </summary>
    abstract GetAllContainers : unit -> Async<string []>


    /// <summary>
    ///     Persists a value to the underlying store and returns a typed cloud reference.
    /// </summary>
    /// <param name="uri">Storage uri to be used.</param>
    /// <param name="value">Value to be persisted.</param>
    abstract CreateCloudRef : uri:string * value:'T -> Async<ICloudRef<'T>>

    /// <summary>
    ///     Persists a collection of values to the underlying store and return a typed cloud sequence.
    /// </summary>
    /// <param name="uri">Storage uri to be used.</param>
    /// <param name="values">Values to be persisted.</param>
    abstract CreateCloudSeq : uri:string * values:seq<'T> -> Async<ICloudSeq<'T>>

    /// <summary>
    ///     Persists a value to the underlying store and returns a typed mutable cloud reference.
    /// </summary>
    /// <param name="uri">Storage uri to be used.</param>
    /// <param name="value">Initial value.</param>
    abstract CreateMutableCloudRef : uri:string * value:'T -> Async<IMutableCloudRef<'T>>

    /// <summary>
    ///     Stores a collection of values to the underlying store and returns a cloud array reference.
    /// </summary>
    /// <param name="uri">Storage uri to be used.</param>
    /// <param name="values">Values to be persisted.</param>
    abstract CreateCloudArray : uri:string * values:seq<'T> -> Async<ICloudArray<'T>>

    /// <summary>
    ///     Combines a collection of persisted cloud arrays into one cloud array instance.
    /// </summary>
    /// <param name="uri">Entry point uri.</param>
    /// <param name="arrays">Arrays to be combined.</param>
    abstract CombineCloudArrays : uri:string * arrays:seq<ICloudArray<'T>> -> Async<ICloudArray<'T>>

    /// <summary>
    ///     Serializes binary data to a file in the underlying store and returns a cloud file reference.
    /// </summary>
    /// <param name="uri">Storage uri to be used.</param>
    /// <param name="serializer">Asynchronous serialization function.</param>
    abstract CreateCloudFile : uri:string * serializer:(System.IO.Stream -> Async<unit>) -> Async<ICloudFile>

    /// <summary>
    ///     Returns all files contained in given container in the underlying storage as cloud files.
    /// </summary>
    /// <param name="container"></param>
    abstract GetContainedFiles : container:string -> Async<ICloudFile []>

    /// <summary>
    ///     Returns a cloud file found on provided uri, if it exists.
    /// </summary>
    /// <param name="uri">Uri to be looked up.</param>
    abstract GetCloudFile : uri:string -> Async<ICloudFile>