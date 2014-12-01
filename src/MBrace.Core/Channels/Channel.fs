namespace Nessos.MBrace.Channels

open Nessos.MBrace
open Nessos.MBrace.Continuation

type IChannel<'T> =
    inherit ICloudDisposable
    abstract Send : message:'T -> Async<unit>
    abstract Receive : ?timeout:int -> Async<'T>

type IChannelProvider =
    abstract CreateChannel<'T> : unit -> Async<IChannel<'T>>

#nowarn "444"

type Channel =

    static member New<'T>() = cloud {
        let! chanP = Cloud.GetResource<IChannelProvider> ()
        return! Cloud.OfAsync <| chanP.CreateChannel<'T> ()
    }

    static member Send<'T> (message : 'T) (channel : IChannel<'T>) = cloud {
        return! Cloud.OfAsync <| channel.Send message
    }

    static member Receive<'T> (channel : IChannel<'T>, ?timeout : int) = cloud {
        return! Cloud.OfAsync <| channel.Receive (?timeout = timeout)
    }

    static member Dispose (channel : IChannel<'T>) = cloud {
        return! Cloud.OfAsync <| (channel :> ICloudDisposable).Dispose()
    }