namespace MBrace.Runtime

open System
open System.Collections
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Reflection

open Nessos.FsPickler
open Nessos.FsPickler.Hashing
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.String
open MBrace.Runtime.InMemoryRuntime
open MBrace.Runtime.Vagabond

////
////  TODO : should be replaced by distributed caching
////
//
//[<NoEquality; NoComparison>]
//type ObjectRepresentation<'T> =
//    | Intact of 'T
//    | Sifted of HashSift<'T>
//
//[<AutoSerializable(false)>]
//type CloudValueManager(storeConfig : CloudFileStoreConfiguration, container : string, serializer : ISerializer, cache : IObjectCache) =
//    let cvalues = new ConcurrentDictionary<HashResult, CloudValue<obj>> ()
//
//    let imem = LocalRuntime.Create(fileConfig = storeConfig, serializer = serializer, objectCache = cache)
//
//    let getPathByHash(hash : HashResult) = local {
//        let truncate n (text : string) = 
//            if text.Length <= n then text
//            else text.Substring(0, n)
//
//        let lbytes = BitConverter.GetBytes(int hash.Length)
//        let base32 = Convert.BytesToBase32 (Array.append lbytes hash.Hash)
//        let typeName = hash.Type.Split([|',';'`' |]).[0] |> truncate 10
//        let fileName = sprintf "%s-%s.val" typeName base32
//        return! CloudPath.Combine(container, fileName)
//    }
//
//    let tryGetCloudValue (hash : HashResult) = local {
//        let ok, cval = cvalues.TryGetValue hash
//        if ok then return Some cval
//        else
//            let! path = getPathByHash hash
//            try
//                let! cval = CloudValue.OfCloudFile<obj>(path, enableCache = true)
//                return cvalues.GetOrAdd(hash, cval) |> Some
//
//            with :? FileNotFoundException -> 
//                return None
//    }
//    
//    member __.TryGetValueByHash(hash : HashResult) = 
//        local {
//            match VagabondRegistry.Instance.TryGetBindingByHash hash with
//            | Some fI -> return fI.GetValue() |> Some
//            | None ->
//                let! cval = tryGetCloudValue hash
//                match cval with
//                | None -> return None
//                | Some cval -> let! value = cval.Value in return Some value
//        } |> imem.RunAsync
//
//    member __.GetValueByHash(hash : HashResult) = async {
//        let! value = __.TryGetValueByHash hash
//        match value with
//        | None -> return raise <| new FileNotFoundException(sprintf "%A" hash)
//        | Some v -> return v
//    }
//
//    member __.ContainsValue(hash : HashResult) = 
//        local {
//            match VagabondRegistry.Instance.TryGetBindingByHash hash with
//            | Some _ -> return true
//            | None ->
//                let! cval = tryGetCloudValue hash
//                return Option.isSome cval
//        } |> imem.RunAsync
//
//    member __.UploadValue (hash : HashResult, value:obj) = 
//        local {
//            match VagabondRegistry.Instance.TryGetBindingByHash hash with
//            | Some _ -> return ()
//            | None ->
//                let! cval = tryGetCloudValue hash
//                match cval with
//                | Some _ -> return ()
//                | None ->
//                    let! path = getPathByHash hash
//                    let! cval = CloudValue.New<obj>(value, path = path, enableCache = true)
//                    ignore <| cvalues.TryAdd(hash, cval)
//                    return ()
//
//        } |> imem.RunAsync
//
//    member cv.TrySiftObject (graph : 'T, ?siftThreshold:int64) : Async<ObjectRepresentation<'T>> = async {
//        let siftThreshold = defaultArg siftThreshold (5L * 1024L * 1024L)
//        let serializer = VagabondRegistry.Instance.Serializer
//        if serializer.ComputeSize graph < 2L * siftThreshold then return Intact graph else
//
//        let siftValue (obj:obj) (hash:HashResult) =
//            if hash.Length < siftThreshold then false
//            elif cv.ContainsValue hash |> Async.RunSync then true
//            else
//                match obj with
//                | :? Array | :? ICollection -> true
//                | _ -> false
//
//        let sifted, hashObjs = serializer.HashSift(graph, siftValue)
//        if hashObjs.Length = 0 then return Intact graph else
//
//        do! hashObjs |> Seq.map (fun (o,h) -> cv.UploadValue(h,o)) |> Async.Parallel |> Async.Ignore
//
//        return Sifted sifted
//    }
//
//    member cv.UnsiftObject(orepr : ObjectRepresentation<'T>) = async {
//        match orepr with
//        | Intact t -> return t
//        | Sifted sifted ->
//            let! hashObjs =
//                sifted.Hashes
//                |> Seq.map (fun h -> async { let! obj = cv.GetValueByHash h in return (obj,h)})
//                |> Async.Parallel
//
//            return VagabondRegistry.Instance.Serializer.HashUnsift(sifted, hashObjs)
//    }