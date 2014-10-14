namespace Nessos.MBrace

    type IResultAggregator<'T> =
        abstract Item : index:int -> 'T with set
        abstract CheckIsComplete : unit -> bool
        abstract ToArray : unit -> 'T []

    and IResultAggregatorFactory =
        abstract Create<'T> : capacity : int -> IResultAggregator<'T>


    type InMemoryAggregator<'T> (capacity : int) =
        [<VolatileField>]
        let mutable counter = 0
        let array = Array.zeroCreate<'T> capacity

        interface IResultAggregator<'T> with
            member __.Item with set i t = array.[i] <- t
            member __.CheckIsComplete () = System.Threading.Interlocked.Increment &counter = capacity
            member __.ToArray () = array

    and InMemoryAggregatorFactory() =
        interface IResultAggregatorFactory with
            member __.Create<'T> capacity = new InMemoryAggregator<'T>(capacity) :> IResultAggregator<'T>