namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Store
open MBrace.Workflows
open MBrace.Client

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudChannel Tests`` (parallelismFactor : int) as self =

    let runRemote wf = self.Run wf 
    let runLocal wf = self.RunLocal wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocal : Cloud<'T> -> 'T
    /// Local store client instance
    abstract ChannelClient : CloudChannelClient


    [<Test>]
    member __.``Local StoreClient`` () =
        let cc = __.ChannelClient
        let sp, rp = cc.Create()
        cc.Send (sp, 42)
        cc.Receive rp |> shouldEqual 42

    [<Test>]
    member __.``Channels: simple send/receive`` () =
        cloud {
            let! send,recv = CloudChannel.New<int> ()
            let! _,value = CloudChannel.Send(send, 42) <||> CloudChannel.Receive recv
            return value
        } |> runRemote |> shouldEqual 42

    [<Test>]
    member __.``Channels: multiple send/receive`` () =
        cloud {
            let! sp,rp = CloudChannel.New<int option> ()
            let rec sender n = cloud {
                if n = 0 then
                    do! CloudChannel.Send(sp, None)
                else
                    do! CloudChannel.Send(sp, Some n)
                    return! sender (n-1)
            }

            let rec receiver c = cloud {
                let! v = CloudChannel.Receive rp
                match v with
                | None -> return c
                | Some i -> return! receiver (c + i)
            }

            let! _, result = sender 100 <||> receiver 0
            return result
        } |> runRemote |> shouldEqual 5050

    [<Test>]
    member __.``Channels: multiple senders`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! sp, rp = CloudChannel.New<int> ()
            let sender n = cloud {
                for i in [1 .. n] do
                    do! CloudChannel.Send(sp, i)
            }

            let rec receiver c n = cloud {
                if n = 0 then return c
                else
                    let! i = CloudChannel.Receive rp
                    return! receiver (c + 1) (n - 1)
            }

            let senders = Seq.init parallelismFactor (fun _ -> sender 10) |> Cloud.Parallel |> Cloud.Ignore
            let! _,result = senders <||> receiver 0 (parallelismFactor * 10)
            return result
        } |> runRemote |> shouldEqual (parallelismFactor * 10)
