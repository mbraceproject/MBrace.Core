namespace MBrace.Runtime.InMemory

open MBrace

open System
open System.Threading

open MBrace
open MBrace.Store
open MBrace.Continuation

[<AutoSerializable(false)>]
type private InMemoryAtom<'T> (initial : 'T) =
    let id = Guid.NewGuid().ToString("N")
    let container = ref (Some initial)

    let rec swap (f : 'T -> 'T) = 
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | cv ->
            let result = Interlocked.CompareExchange<'T option>(container, Option.map f cv, cv)
            if obj.ReferenceEquals(result, cv) then ()
            else Thread.SpinWait 20; swap f

    let force (t : 'T) =
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | _ -> container := Some t

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = cloud { return Option.get container.Value }
        member __.Update(updater, ?maxRetries) = cloud { return swap updater }
        member __.Force(value) = cloud { return force value }
        member __.Dispose () = cloud { return container := None }

[<Sealed; AutoSerializable(false)>]
type InMemoryAtomProvider () =
    let id = Guid.NewGuid().ToString("N")

    static member CreateConfiguration () : CloudAtomConfiguration =
        {
            AtomProvider = new InMemoryAtomProvider() :> ICloudAtomProvider
            DefaultContainer = ""
        }

    interface ICloudAtomProvider with
        member __.Name = "InMemoryAtomProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = Guid.NewGuid().ToString("N")
        member __.IsSupportedValue _ = true
        member __.CreateAtom<'T>(_, init : 'T) = async { return new InMemoryAtom<'T>(init) :> _ }
        member __.DisposeContainer _ = raise <| new NotImplementedException()

/// Defines an in-memory channel factory using mailbox processor
[<Sealed; AutoSerializable(false)>]
type InMemoryChannelProvider () =
    let id = Guid.NewGuid().ToString("N")

    static member CreateConfiguration () : CloudChannelConfiguration =
        {
            ChannelProvider = new InMemoryChannelProvider() :> ICloudChannelProvider
            DefaultContainer = ""
        }

    interface ICloudChannelProvider with
        member __.Name = "InMemoryChannelProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = Guid.NewGuid().ToString("N")

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| Guid.NewGuid().ToString()
            let mbox = Microsoft.FSharp.Control.MailboxProcessor<'T>.Start(fun _ -> async.Zero())
            let sender =
                {
                    new ISendPort<'T> with
                        member __.Id = id
                        member __.Send(msg : 'T) = cloud { return mbox.Post msg }
                }

            let receiver =
                {
                    new IReceivePort<'T> with
                        member __.Id = id
                        member __.Receive(?timeout : int) = cloud { return! Cloud.OfAsync <| mbox.Receive(?timeout = timeout) }
                        member __.Dispose() = raise <| new NotSupportedException()
                }

            return sender, receiver
        }

        member __.DisposeContainer _ = async.Zero()