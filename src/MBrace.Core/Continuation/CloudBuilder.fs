namespace MBrace

//  Cloud builder implementations

open MBrace.Continuation

[<AutoOpen>]
module internal CloudBuilderUtils =

    let inline Body f = new Cloud<_>(f)
    let inline (|Body|) (f : Cloud<_>) = f.Body

    let inline capture (e : 'exn) = ExceptionDispatchInfo.Capture e
    let inline extract (edi : ExceptionDispatchInfo) = edi.Reify(false, false)
    let protect f s = try Choice1Of2 <| f s with e -> Choice2Of2 e
    let inline getMetadata (t : 'T) = t.GetType().FullName
    let inline appendToStacktrace functionName (edi : ExceptionDispatchInfo) =
        let entry = sprintf "   at %s" functionName
        edi.AppendToStackTrace entry
        

    type Continuation<'T> with
        member inline c.Cancel ctx = c.Cancellation ctx (new System.OperationCanceledException())

        member inline c.Choice (ctx, choice : Choice<'T, exn>) =
            match choice with
            | Choice1Of2 t -> c.Success ctx t
            | Choice2Of2 e -> c.Exception ctx (capture e)

        member inline c.Choice (ctx, choice : Choice<Cloud<'T>, exn>) =
            match choice with
            | Choice1Of2 (Body f) -> f ctx c
            | Choice2Of2 e -> c.Exception ctx (capture e)

    type ExecutionContext with
        member inline ctx.IsCancellationRequested = ctx.CancellationToken.IsCancellationRequested


    let inline ret t = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Success ctx t)
    let inline raiseM<'T> e : Cloud<'T> = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Exception ctx (capture e))
    let inline ofAsync (asyncWorkflow : Async<'T>) = 
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            Async.StartWithContinuations(asyncWorkflow, cont.Success ctx, capture >> cont.Exception ctx, cont.Cancellation ctx, ctx.CancellationToken))

    let zero = ret ()

    let inline bind (Body f : Cloud<'T>) (g : 'T -> Cloud<'S>) : Cloud<'S> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = 
                    fun ctx t ->
                        if ctx.IsCancellationRequested then cont.Cancel ctx
                        elif Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> cont.Choice(ctx, protect g t))
                        else
                            cont.Choice(ctx, protect g t)

                Exception = 
                    fun ctx e -> 
                        if ctx.IsCancellationRequested then cont.Cancel ctx
                        elif Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> cont.Exception ctx e)
                        else
                            cont.Exception ctx e

                Cancellation = cont.Cancellation
            }

            if Trampoline.IsBindThresholdReached() then 
                Trampoline.QueueWorkItem (fun () -> f ctx cont')
            else
                f ctx cont'
        )

    let inline tryWith (Body f : Cloud<'T>) (handler : exn -> Cloud<'T>) : Cloud<'T> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = 
                    fun ctx t -> 
                        if ctx.IsCancellationRequested then cont.Cancel ctx
                        elif Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> cont.Success ctx t)
                        else
                            cont.Success ctx t
                
                Exception =
                    fun ctx edi ->
                        if ctx.IsCancellationRequested then cont.Cancel ctx
                        elif Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> cont.Choice(ctx, protect handler (extract edi)))
                        else
                            cont.Choice(ctx, protect handler (extract edi))

                Cancellation = cont.Cancellation
            }

            if Trampoline.IsBindThresholdReached() then 
                Trampoline.QueueWorkItem (fun () -> f ctx cont')
            else
                f ctx cont'
        )

    let inline tryFinally (Body f : Cloud<'T>) (Body finalizer : Cloud<unit>) : Cloud<'T> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else

            let cont' = {
                Success =
                    fun ctx t -> 
                        if ctx.IsCancellationRequested then cont.Cancel ctx else
                        let cont' = Continuation.map (fun () -> t) cont
                        if Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> finalizer ctx cont')
                        else
                            finalizer ctx cont'

                Exception = 
                    fun ctx edi -> 
                        if ctx.IsCancellationRequested then cont.Cancel ctx else
                        let cont' = Continuation.failwith (fun () -> (extract edi)) cont
                        if Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> finalizer ctx cont')
                        else
                            finalizer ctx cont'

                Cancellation = cont.Cancellation
            }

            if Trampoline.IsBindThresholdReached() then 
                Trampoline.QueueWorkItem (fun () -> f ctx cont')
            else
                f ctx cont'
        )

    let inline combine (f : Cloud<unit>) (g : Cloud<'T>) : Cloud<'T> = bind f (fun () -> g)
    let inline delay (f : unit -> Cloud<'T>) : Cloud<'T> = bind zero f
    let inline using<'T, 'S when 'T :> ICloudDisposable> (t : 'T) (g : 'T -> Cloud<'S>) : Cloud<'S> =
        tryFinally (bind (ret t) g) (delay t.Dispose)

    let inline forM (body : 'T -> Cloud<unit>) (ts : 'T []) : Cloud<unit> =
        let rec loop i () =
            if i = ts.Length then zero
            else
                match protect body ts.[i] with
                | Choice1Of2 b -> bind b (loop (i+1))
                | Choice2Of2 e -> raiseM e

        delay (loop 0)

    let inline whileM (pred : unit -> bool) (body : Cloud<unit>) : Cloud<unit> =
        let rec loop () =
            match protect pred () with
            | Choice1Of2 true -> bind body loop
            | Choice1Of2 false -> zero
            | Choice2Of2 e -> raiseM e

        delay loop

/// Cloud workflow expression builder
type CloudBuilder () =
    member __.Return (t : 'T) = ret t
    member __.Zero () = zero
    member __.Delay (f : unit -> Cloud<'T>) : Cloud<'T> = delay f
    member __.ReturnFrom (c : Cloud<'T>) = c
    member __.Combine(f : Cloud<unit>, g : Cloud<'T>) = combine f g
    member __.Bind (f : Cloud<'T>, g : 'T -> Cloud<'S>) : Cloud<'S> = bind f g
    member __.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> = using value bindF

    member __.TryWith(f : Cloud<'T>, handler : exn -> Cloud<'T>) : Cloud<'T> = tryWith f handler
    member __.TryFinally(f : Cloud<'T>, finalizer : unit -> unit) : Cloud<'T> = 
        tryFinally f (delay (fun () -> ret (finalizer ())))

    member __.For(ts : 'T [], body : 'T -> Cloud<unit>) : Cloud<unit> = forM body ts
    member __.For(ts : seq<'T>, body : 'T -> Cloud<unit>) : Cloud<unit> = forM body (Seq.toArray ts)

    [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 444)>]
    member __.While(pred : unit -> bool, body : Cloud<unit>) : Cloud<unit> = whileM pred body

/// Cloud builder module
[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CloudBuilder =
        
    /// cloud builder instance
    let cloud = new CloudBuilder ()