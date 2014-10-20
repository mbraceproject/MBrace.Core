namespace Nessos.MBrace

#nowarn "444"

open System.IO

open Nessos.MBrace.Runtime

/// Cloud reference methods.
type CloudRef =

    /// <summary>
    ///     Creates a new cloud reference to the underlying store with provided value.
    ///     Cloud references are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud reference value.</param>
    static member New(value : 'T) : Cloud<ICloudRef<'T>> = cloud {
        let! provider = Cloud.GetResource<IStorageProvider> ()
        let uri = provider.GetRandomUri()
        return! Cloud.OfAsync <| provider.CreateCloudRef(uri, value)
    }

    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    static member Read(cloudRef : ICloudRef<'T>) : Cloud<'T> = Cloud.OfAsync <| cloudRef.GetValue()

/// Cloud sequence methods.
type CloudSeq =

    /// <summary>
    ///     Creates a new cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Collection to populate the cloud sequence with.</param>
    static member New(values : seq<'T>) : Cloud<ICloudSeq<'T>> = cloud {
        let! provider = Cloud.GetResource<IStorageProvider> ()
        let uri = provider.GetRandomUri()
        return! Cloud.OfAsync <| provider.CreateCloudSeq(uri, values)
    }

/// Mutable cloud reference methods.
type MutableCloudRef =
    
    /// <summary>
    ///     Creates a new mutable cloud ref with given initial value.
    ///     Mutable cloud refs are not cached locally.
    /// </summary>
    /// <param name="init">Value to initialize mutable cloud ref with.</param>
    static member New(init : 'T) : Cloud<IMutableCloudRef<'T>> = cloud {
        let! provider = Cloud.GetResource<IStorageProvider> ()
        let uri = provider.GetRandomUri()
        return! Cloud.OfAsync <| provider.CreateMutableCloudRef(uri, init)
    }

    /// <summary>
    ///     Returns the current value of the mutable cloud reference.
    /// </summary>
    /// <param name="mref">Mutable cloud ref to be dereferenced.</param>
    static member Read(mref : IMutableCloudRef<'T>) : Cloud<'T> = Cloud.OfAsync(mref.GetValue())

    /// <summary>
    ///     Attempt to update the value of given reference with given value.
    ///     Returns true iff the operation was successful.
    /// </summary>
    /// <param name="mref">Mutable cloud reference to be updated.</param>
    /// <param name="value">Value to be set.</param>
    static member TryUpdate(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<bool> = Cloud.OfAsync(mref.TryUpdate value)

    /// <summary>
    ///     Force update to the value of given reference with given value.
    /// </summary>
    /// <param name="mref">Mutable cloud reference to be updated.</param>
    /// <param name="value">Value to be set.</param>
    static member ForceUpdate(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<unit> = Cloud.OfAsync(mref.ForceUpdate value)

    /// <summary>
    ///     Updates the MutableCloudRef using the update function.
    ///     This method will return when the update is successful.
    /// </summary>
    /// <param name="mref">The MutableCloudRef to be updated.</param>
    /// <param name="update">
    ///     A function that takes the current value of the MutableCloudRef and
    ///     returns the new value to be stored.
    /// </param>
    /// <param name="interval">The interval, in milliseconds, to sleep between the spin calls.</param>
    static member SpinSet<'T>(mref : IMutableCloudRef<'T>, update : 'T -> 'T, ?interval : int) : Cloud<unit> = 
        let rec spin retries interval = cloud {
            let! current = MutableCloudRef.Read mref
            let! isSuccess = MutableCloudRef.TryUpdate(mref, update current)
            if isSuccess then return ()
            else
                do! Cloud.Sleep interval
                return! spin (retries + 1) interval
        }

        spin 0 (defaultArg interval 0)

/// Cloud array API
type CloudArray =
    
    /// <summary>
    ///     Creates a new CloudArray in the specified container.
    /// </summary>
    /// <param name="values">Values to be stored.</param>
    /// <param name="uri">Target uri for given cloud array. Defaults to runtime-assigned path.</param>
    static member New<'T>(values : seq<'T>, ?uri : string) : Cloud<ICloudArray<'T>> = cloud {
        let! provider = Cloud.GetResource<IStorageProvider> ()
        let uri = match uri with None -> provider.GetRandomUri() | Some u -> u
        return! Cloud.OfAsync <| provider.CreateCloudArray(uri, values)
    }

    /// <summary>
    ///     Combines a collection of cloud arrays into one.
    /// </summary>
    /// <param name="arrays">Cloud arrays to be combined.</param>
    static member Combine(arrays : seq<ICloudArray<'T>>) : Cloud<ICloudArray<'T>> = cloud {
        let! provider = Cloud.GetResource<IStorageProvider> ()
        let uri = provider.GetRandomUri()
        return! Cloud.OfAsync <| provider.CombineCloudArrays(uri, arrays)
    }
        

/// Cloud file API
type CloudFile =
    
        /// <summary> 
        ///     Create a new file in the storage with the specified folder and name.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="serializer">Function that will write data on the underlying stream.</param>
        /// <param name="uri">Target uri for given cloud file. Defaults to runtime-assigned path.</param>
        static member New(serializer : Stream -> Async<unit>, ?uri : string) : Cloud<ICloudFile> = cloud {
            let! provider = Cloud.GetResource<IStorageProvider> ()
            let uri = match uri with None -> provider.GetRandomUri() | Some u -> u
            return! Cloud.OfAsync <| provider.CreateCloudFile(uri, serializer)
        }

        /// <summary> 
        ///     Read the contents of a CloudFile using the given deserialize/reader function.
        /// </summary>
        /// <param name="cloudFile">CloudFile to read.</param>
        /// <param name="deserializer">Function that reads data from the underlying stream.</param>
        static member Read(cloudFile : ICloudFile, deserializer : Stream -> Async<'T>) : Cloud<'T> = 
            Cloud.OfAsync(async { use! stream = cloudFile.Read() in return! deserializer stream })

        /// <summary> 
        ///     Returns all CloudFiles in given container.
        /// </summary>
        /// <param name="container">The container (folder) to search.</param>
        static member Enumerate(container : string) : Cloud<ICloudFile []> = cloud {
            let! provider = Cloud.GetResource<IStorageProvider> ()
            return! Cloud.OfAsync <| provider.GetContainedFiles container
        }