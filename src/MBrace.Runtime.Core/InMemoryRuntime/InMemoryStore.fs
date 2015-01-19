namespace MBrace.Runtime.InMemory

open MBrace

open System
open System.Threading

open MBrace
open MBrace.Store
open MBrace.Continuation

type private Box<'T> = { Value : 'T }

[<AutoSerializable(false)>]
type private InMemoryAtom<'T> (initial : 'T) =
    let id = Guid.NewGuid().ToString("N")
    let container = ref { Value = initial }

    let rec swap (f : 'T -> 'T) = 
        let cv = container.Value
        let result = Interlocked.CompareExchange<Box<'T>>(container, { Value = f cv.Value }, cv)
        if obj.ReferenceEquals(result, cv) then ()
        else Thread.SpinWait 20; swap f

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = cloud { return container.Value.Value }
        member __.Update(updater, ?maxRetries) = cloud { return swap updater }
        member __.Force(value) = cloud { return container := { Value = value } }
        member __.Dispose () = cloud.Zero()

[<AutoSerializable(false)>]
type InMemoryAtomProvider () =
    let id = Guid.NewGuid().ToString("N")

    static member CreateConfiguration () : AtomConfiguration =
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
        member __.DisposeContainer _ = async.Zero()

/// Defines an in-memory channel factory using mailbox processor
[<AutoSerializable(false)>]
type InMemoryChannelProvider () =
    let id = Guid.NewGuid().ToString("N")

    static member CreateConfiguration () : ChannelConfiguration =
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
                        member __.Dispose() = cloud.Zero()
                }

            return sender, receiver
        }

        member __.DisposeContainer _ = async.Zero()