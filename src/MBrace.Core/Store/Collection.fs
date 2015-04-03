namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace
open MBrace.Store
open MBrace.Continuation

#nowarn "444"

type ICloudCollection<'T> =
    abstract Count : Local<int64>
    abstract ToEnumerable : unit -> Local<seq<'T>>

type IPartitionedCollection<'T> =
    inherit ICloudCollection<'T>
    abstract PartitionCount : Local<int>
    abstract GetPartitions : unit -> Local<ICloudCollection<'T> []>

type IPartitionableCollection<'T> =
    inherit ICloudCollection<'T>
    abstract GetPartitions : partitionCount:int -> Local<ICloudCollection<'T> []>