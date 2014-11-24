namespace Nessos.MBrace.Store

open System
open System.Collections
open System.Collections.Generic
open System.IO

open Nessos.MBrace
open Nessos.MBrace.Runtime

[<Sealed; AutoSerializable(true)>]
type CloudSeq<'T> private (serializerId : string, length : int, source : CloudFile) =
    
    let getSequenceAsync() = async {
        let serializer = Dependency.Resolve<ISerializer> serializerId
        let! stream = source.BeginRead()
        return serializer.SeqDeserialize<'T>(stream, length)
    }

    let getSequence () = getSequenceAsync () |> Async.RunSync

    member __.Id = source.Path
    member __.File = source
    member __.Length = length

    member __.GetSequenceAsync() = getSequenceAsync()

    interface ICloudDisposable with
        member __.Dispose() = (source :> ICloudDisposable).Dispose()

    interface IEnumerable<'T> with
        member __.GetEnumerator() = (getSequence() :> IEnumerable).GetEnumerator()
        member __.GetEnumerator() = getSequence().GetEnumerator()

    static member internal Create (values : seq<'T>, config : CloudStoreConfiguration) = async {
        let fileName = config.FileProvider.CreateUniqueFileName "TODO : implement process-bound container name"
        let size = ref 0
        let! file = config.FileProvider.CreateFile(fileName, fun stream -> async { size := config.Serializer.SeqSerialize(stream, values) })
        return new CloudSeq<'T>(config.Serializer.UUID, !size, file)
    }

//type CloudSeq =
//    static member New<'T>(value : 'T, provider : StoreProvider) = CloudRef<'T>.Create(value, provider)