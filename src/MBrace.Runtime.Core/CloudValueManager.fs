namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.IO
open System.Reflection

open Nessos.FsPickler
open Nessos.FsPickler.Hashing
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Client
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Vagabond

type CloudValueManager(container : string) =
    let cvalues = new ConcurrentDictionary<HashResult, CloudValue<obj>> ()

    let getPathByHash(hash : HashResult) = local {
        let truncate n (text : string) = 
            if text.Length <= n then text
            else text.Substring(0, n)

        let lbytes = BitConverter.GetBytes(int hash.Length)
        let base32 = Convert.BytesToBase32 (Array.append lbytes hash.Hash)
        let typeName = hash.Type.Split([|',';'`' |]).[0] |> truncate 10
        let fileName = sprintf "%s-%s.val" typeName base32
        return! CloudPath.Combine(container, fileName)
    }
    
    member __.TryGetValueByHash(hash : HashResult) = local {
        match VagabondRegistry.Instance.TryGetBindingByHash hash with
        | Some fI -> return fI.GetValue() |> Some
        | None ->
            let ok, cval = cvalues.TryGetValue hash
            if ok then let! value = cval.Value in return Some value
            else
                let! path = getPathByHash hash
                try
                    let! cval = CloudValue.OfCloudFile<obj>(path, enableCache = true)
                    let cval = cvalues.GetOrAdd(hash, cval)
                    let! value = cval.Value
                    return Some value

                with :? FileNotFoundException -> return None
    }

    member __.ContainsValue(hash : HashResult) = local {
        match VagabondRegistry.Instance.TryGetBindingByHash hash with
        | Some _ -> return true
        | None ->
            let! path = getPathByHash hash
            return! CloudFile.Exists path
    }

    member __.UploadValue (hash : HashResult, value:obj) = local {
        match VagabondRegistry.Instance.TryGetBindingByHash hash with
        | Some _ -> return ()
        | None ->
            let! path = getPathByHash hash
            let! exists = CloudFile.Exists path
            if not exists then
                let! cval = CloudValue.New<obj>(value, path = path, enableCache = true)
                let _ = cvalues.GetOrAdd(hash, cval)
                return ()
    }