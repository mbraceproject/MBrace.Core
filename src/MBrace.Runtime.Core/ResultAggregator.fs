namespace MBrace.Runtime

open System

open MBrace.Core
open MBrace.Store
open MBrace.Store.Internals

open MBrace.Runtime.Utils

type IResultAggregator<'T> =
    abstract Capacity : int
    abstract IsCompleted : Local<bool>
    abstract SetResult : index:int * value:'T * overwrite:bool -> Local<bool>
    abstract ToArray : unit -> Local<'T []>

type StoreResultAggregator<'T> private (container : string, atom : ICloudAtom<Map<int, CloudValue<'T>>>, capacity : int) = 
    interface IResultAggregator<'T> with
        member __.Capacity = capacity
        member __.IsCompleted = local {
            let! value = atom.Value
            return value.Count = capacity
        }

        member __.SetResult(index : int, value : 'T, overwrite : bool) = local {
            if index < 0 || index >= capacity then raise <| new IndexOutOfRangeException()
            let! fileName = CloudPath.GetRandomFileName container
            let! cval = CloudValue.New(value, path = fileName, enableCache = false)
            let updater (map : Map<int, CloudValue<'T>>) =
                if not overwrite && map.ContainsKey index then invalidOp <| sprintf "Result at position %d has already been set." index
                let map = map.Add(index, cval)
                map.Count, map

            let! value = CloudAtom.Transact(atom, updater)
            return value = capacity
        }

        member __.ToArray() = local {
            let! values = atom.Value
            if values.Count <> capacity then invalidOp "Result aggregator has not completed aggregation."
            return! values |> Seq.map (fun kv -> kv.Value.Value) |> Local.Parallel
        }

    static member internal Create(container : string, capacity : int) = local {
        let! atom = CloudAtom.New Map.empty
        return new StoreResultAggregator<'T>(container, atom, capacity)
    }

type StoreResultAggregator =
    static member Create<'T>(container : string, capacity : int) = StoreResultAggregator<'T>.Create(container, capacity)