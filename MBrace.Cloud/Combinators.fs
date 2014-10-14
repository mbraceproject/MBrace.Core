namespace Nessos.MBrace

    type Cloud =

        static member FromContinuations(body : Context<'T> -> unit) : Cloud<'T> = Body body
        static member OfAsync(asyncWorkflow : Async<'T>) : Cloud<'T> = 
            Body(fun ctx -> Async.StartWithContinuations(asyncWorkflow, ctx.scont, ctx.econt, ctx.ccont))

        static member StartWithContext((Body f) : Cloud<'T>, ctx : Context<'T>) = f ctx
        
        static member RunLocalAsync(cloudWorkflow : Cloud<'T>, ?resources : IResourceResolver) : Async<'T> = async {
            let tcs = new System.Threading.Tasks.TaskCompletionSource<'T>()
            let context = {
                Resource = 
                    match resources with
                    | None -> ResourceResolverFactory.CreateEmptyResolver ()
                    | Some r -> r

                scont = tcs.SetResult
                econt = tcs.SetException
                ccont = fun _ -> tcs.SetCanceled ()
            }

            do Cloud.StartWithContext(cloudWorkflow, ctx = context)

            try return! Async.AwaitTask tcs.Task
            with :? System.AggregateException as e ->
                return raise e.InnerException
        }

        static member RunLocal(cloudWorkflow : Cloud<'T>, ?resources : IResourceResolver) : 'T =
            Cloud.RunLocalAsync(cloudWorkflow, ?resources = resources) |> Async.RunSynchronously