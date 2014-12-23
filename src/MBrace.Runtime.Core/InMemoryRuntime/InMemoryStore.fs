namespace Nessos.MBrace.Runtime.InMemory

open Nessos.MBrace

open System
open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

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
        member __.GetValue () = async { return container.Value.Value }
        member __.Value = container.Value.Value
        member __.Update(updater, ?maxRetries) = async { return swap updater }
        member __.Force(value) = async { return container := { Value = value } }
        member __.Dispose () = async.Zero()

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

        member __.CreateChannel<'T> (_ : string) = async {
            let mbox = Microsoft.FSharp.Control.MailboxProcessor<'T>.Start(fun _ -> async.Zero())
            let sender =
                {
                    new ISendPort<'T> with
                        member __.Send(msg : 'T) = async { return mbox.Post msg }
                }

            let receiver =
                {
                    new IReceivePort<'T> with
                        member __.Receive(?timeout : int) = mbox.Receive(?timeout = timeout)
                        member __.Dispose() = async.Zero()
                }

            return sender, receiver
        }

        member __.DisposeContainer _ = async.Zero()