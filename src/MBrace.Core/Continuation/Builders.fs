namespace MBrace

//  Cloud builder implementation

open System
open MBrace.Continuation

[<AutoOpen>]
module internal CloudBuilderImpl =

    let inline Body f = new Workflow<_,_>(f)
    let inline (|Body|) (f : #Workflow<_>) = f.Body

    let inline plug (f : #Workflow<'T>) : Workflow<'C2, 'T> = Body(f.Body)

    let inline capture (e : 'exn) = ExceptionDispatchInfo.Capture e
    let inline extract (edi : ExceptionDispatchInfo) = edi.Reify(false, false)
    let inline protect f s = try Choice1Of2 <| f s with e -> Choice2Of2 e
    let inline getMetadata (t : 'T) = t.GetType().FullName
    let inline appendToStacktrace functionName (edi : ExceptionDispatchInfo) =
        let entry = sprintf "   at %s" functionName
        edi.AppendToStackTrace entry

    let inline getLocalToken (ctopt : ICloudCancellationToken option) =
        match ctopt with
        | None -> None
        | Some ct -> Some ct.LocalToken

    type Continuation<'T> with
        member inline c.Cancel ctx = c.Cancellation ctx (new System.OperationCanceledException())

        member inline c.Choice (ctx, choice : Choice<'T, exn>) =
            match choice with
            | Choice1Of2 t -> c.Success ctx t
            | Choice2Of2 e -> c.Exception ctx (capture e)

        member inline c.Choice2 (ctx, choice : Choice<#Workflow<'T>, exn>) =
            match choice with
            | Choice1Of2 (Body f) -> f ctx c
            | Choice2Of2 e -> c.Exception ctx (capture e)

    type ExecutionContext with
        member inline ctx.IsCancellationRequested = ctx.CancellationToken.IsCancellationRequested

    let inline ret t = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Success ctx t)
    let inline retFunc (f : unit -> 'T) : Workflow<_,'T> = 
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            match protect f () with
            | Choice1Of2 t -> cont.Success ctx t
            | Choice2Of2 e -> cont.Exception ctx (capture e))

    let czero : Cloud<unit> = ret ()

    let inline raiseM e : Workflow<_,'T> = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Exception ctx (capture e))
    let inline ofAsync (asyncWorkflow : Async<'T>) = 
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            Async.StartWithContinuations(asyncWorkflow, cont.Success ctx, capture >> cont.Exception ctx, cont.Cancellation ctx, ctx.CancellationToken.LocalToken))

    let inline bind (Body f : #Workflow<'T>) (g : 'T -> #Workflow<'S>) : Workflow<_,'S> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = 
                    fun ctx t ->
                        if ctx.IsCancellationRequested then cont.Cancel ctx
                        elif Trampoline.IsBindThresholdReached() then
                            Trampoline.QueueWorkItem(fun () -> cont.Choice2(ctx, protect g t))
                        else
                            cont.Choice2(ctx, protect g t)

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

    let inline tryWith (Body f : #Workflow<'T>) (handler : exn -> #Workflow<'T>) : Workflow<_,'T> =
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
                            Trampoline.QueueWorkItem(fun () -> cont.Choice2(ctx, protect handler (extract edi)))
                        else
                            cont.Choice2(ctx, protect handler (extract edi))

                Cancellation = cont.Cancellation
            }

            if Trampoline.IsBindThresholdReached() then 
                Trampoline.QueueWorkItem (fun () -> f ctx cont')
            else
                f ctx cont'
        )

    let inline tryFinally (Body f : #Workflow<'T>) (Body finalizer : #Workflow<unit>) : Workflow<_,'T> =
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

    let inline combine (f : #Workflow<unit>) (g : #Workflow<'T>) : Workflow<_,'T> = bind f (fun () -> g)
    let inline delay (f : unit -> #Workflow<'T>) : Workflow<_,'T> = bind czero f
    let inline dispose (d : ICloudDisposable) = delay d.Dispose

    let inline usingIDisposable (t : #IDisposable) (g : #IDisposable -> #Workflow<'S>) : Workflow<_,'S> =
        tryFinally (bind (ret t) g) (retFunc t.Dispose)

    let inline usingICloudDisposable (t : #ICloudDisposable) (g : #ICloudDisposable -> #Workflow<'S>) : Workflow<_,'S> =
        tryFinally (bind (ret t) g) (delay t.Dispose)

    let inline forArray (body : 'T -> #Workflow<unit>) (ts : 'T []) : Workflow<_,unit> =
        let rec loop i () =
            if i = ts.Length then czero
            else
                match protect body ts.[i] with
                | Choice1Of2 b -> bind b (loop (i+1))
                | Choice2Of2 e -> raiseM e

        delay (loop 0)

    let inline forList (body : 'T -> #Workflow<unit>) (ts : 'T list) : Workflow<_,unit> =
        let rec loop ts () =
            match ts with
            | [] -> czero
            | t :: ts ->
                match protect body t with
                | Choice1Of2 b -> bind b (loop ts)
                | Choice2Of2 e -> raiseM e

        delay (loop ts)

    let inline forSeq (body : 'T -> #Workflow<unit>) (ts : seq<'T>) : Workflow<_,unit> =
        delay(fun () ->
            let enum = ts.GetEnumerator()
            let rec loop () =
                if enum.MoveNext() then
                    match protect body enum.Current with
                    | Choice1Of2 b -> bind b loop
                    | Choice2Of2 e -> raiseM e
                else
                    ret ()

            tryFinally (loop ()) (retFunc enum.Dispose))

    let inline whileM (pred : unit -> bool) (body : #Workflow<unit>) : Workflow<_,unit> =
        let rec loop () =
            match protect pred () with
            | Choice1Of2 true -> bind body loop
            | Choice1Of2 false -> czero
            | Choice2Of2 e -> raiseM e

        delay loop

/// A collection of builder implementations for MBrace workflows
module Builders =

    /// a Workflow builder that composes arbitrary workflows of identical scheduling contexts
    type GenericWorkflowBuilder () =
        member __.Return (t : 'T) : Workflow<'Ctx, 'T> = ret t
        member __.Zero () : Workflow<'Ctx, unit> = ret ()
        member __.Delay (f : unit -> Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = delay f
        member __.ReturnFrom (c : Workflow<'Ctx,'T>) : Workflow<'Ctx, 'T> = c
        member __.Combine(f : Workflow<'Ctx, unit>, g : Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = combine f g
        member __.Bind (f : Workflow<'Ctx, 'T>, g : 'T -> Workflow<'Ctx, 'S>) : Workflow<'Ctx, 'S> = bind f g

        member __.TryWith(f : Workflow<'Ctx, 'T>, handler : exn -> Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = tryWith f handler
        member __.TryFinally(f : Workflow<'Ctx, 'T>, finalizer : unit -> unit) : Workflow<'Ctx, 'T> = 
            tryFinally f (retFunc finalizer)

        member __.For(ts : 'T [], body : 'T -> Workflow<'Ctx, unit>) : Workflow<'Ctx, unit> = forArray body ts
        member __.For(ts : 'T list, body : 'T -> Workflow<'Ctx, unit>) : Workflow<'Ctx, unit> = forList body ts

    /// A workflow builder that composes workflows of particular scheduling context
    type SpecializedWorkflowBuilder<'Ctx when 'Ctx :> SchedulingContext> () =
        let zero : Workflow<'Ctx, unit> = ret ()
        member __.Return (t : 'T) : Workflow<'Ctx, 'T> = ret t
        member __.Zero () : Workflow<'Ctx, unit> = zero
        member __.Delay (f : unit -> Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = delay f
        member __.ReturnFrom (c : Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = c
        member __.Combine(f : Workflow<'Ctx, unit>, g : Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = combine f g
        member __.Bind (f : Workflow<'Ctx, 'T>, g : 'T -> Workflow<'Ctx, 'S>) : Workflow<'Ctx, 'S> = bind f g

        member __.TryWith(f : Workflow<'Ctx, 'T>, handler : exn -> Workflow<'Ctx, 'T>) : Workflow<'Ctx, 'T> = tryWith f handler
        member __.TryFinally(f : Workflow<'Ctx, 'T>, finalizer : unit -> unit) : Workflow<'Ctx, 'T> = 
            tryFinally f (retFunc finalizer)

        member __.For(ts : 'T [], body : 'T -> Workflow<'Ctx, unit>) : Workflow<'Ctx, unit> = forArray body ts
        member __.For(ts : 'T list, body : 'T -> Workflow<'Ctx, unit>) : Workflow<'Ctx, unit> = forList body ts

    /// Cloud workflow expression builder
    type CloudBuilder () =
        member __.Return (t : 'T) : Cloud<'T> = ret t
        member __.Zero () : Cloud<unit> = czero
        member __.Delay (f : unit -> Cloud<'T>) : Cloud<'T> = delay f
        member __.ReturnFrom (c : Workflow<'T>) : Cloud<'T> = 
            match c with
            | :? Cloud<'T> as c -> c
            | _ -> plug c

        member __.Combine(f : #Workflow<unit>, g : Cloud<'T>) : Cloud<'T> = combine f g
        member __.Bind (f : #Workflow<'T>, g : 'T -> Cloud<'S>) : Cloud<'S> = bind f g

        member __.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> = usingICloudDisposable value bindF

        member __.TryWith(f : Cloud<'T>, handler : exn -> Cloud<'T>) : Cloud<'T> = tryWith f handler
        member __.TryFinally(f : Cloud<'T>, finalizer : unit -> unit) : Cloud<'T> = 
            tryFinally f (retFunc finalizer)

        member __.For(ts : 'T [], body : 'T -> Cloud<unit>) : Cloud<unit> = forArray body ts
        member __.For(ts : 'T list, body : 'T -> Cloud<unit>) : Cloud<unit> = forList body ts
        [<CompilerMessage("For loops indexed on IEnumerable not recommended; consider explicitly converting to list or array instead.", 443)>]
        member __.For(ts : seq<'T>, body : 'T -> Cloud<unit>) : Cloud<unit> = 
            match ts with
            | :? ('T []) as ts -> forArray body ts
            | :? ('T list) as ts -> forList body ts
            | _ -> forSeq body ts

        [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 443)>]
        member __.While(pred : unit -> bool, body : Cloud<unit>) : Cloud<unit> = whileM pred body

    /// Local workflow expression builder
    type LocalBuilder () =
        let lzero : Local<unit> = ret ()
        member __.Return (t : 'T) : Local<'T> = ret t
        member __.Zero () : Local<unit> = lzero
        member __.Delay (f : unit -> Local<'T>) : Local<'T> = delay f
        member __.ReturnFrom (c : Local<'T>) : Local<'T> = c
        member __.Combine(f : Local<unit>, g : Local<'T>) : Local<'T> = combine f g
        member __.Bind (f : Local<'T>, g : 'T -> Local<'S>) : Local<'S> = bind f g

        member __.Using<'T, 'U, 'p when 'T :> IDisposable>(value : 'T, bindF : 'T -> Local<'U>) : Local<'U> = usingIDisposable value bindF
        member __.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Local<'U>) : Local<'U> = usingICloudDisposable value bindF

        member __.TryWith(f : Local<'T>, handler : exn -> Local<'T>) : Local<'T> = tryWith f handler
        member __.TryFinally(f : Local<'T>, finalizer : unit -> unit) : Local<'T> = 
            tryFinally f (retFunc finalizer)

        member __.For(ts : 'T [], body : 'T -> Local<unit>) : Local<unit> = forArray body ts
        member __.For(ts : 'T list, body : 'T -> Local<unit>) : Local<unit> = forList body ts
        member __.For(ts : seq<'T>, body : 'T -> Local<unit>) : Local<unit> = 
            match ts with
            | :? ('T []) as ts -> forArray body ts
            | :? ('T list) as ts -> forList body ts
            | _ -> forSeq body ts

        member __.While(pred : unit -> bool, body : Local<unit>) : Local<unit> = whileM pred body
    

/// Cloud builder module
[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CloudBuilder =

    /// workflow-invariant builder instance
    let internal wfb = new Builders.GenericWorkflowBuilder()
        
    /// cloud builder instance
    let cloud = new Builders.CloudBuilder ()

    /// local builder instance
    let local = new Builders.LocalBuilder ()