namespace Nessos.MBrace

    [<AutoOpen>]
    module internal CloudBuilderUtils =

        let inline protect f s = try Choice1Of2 <| f s with e -> Choice2Of2 e

        type Context<'T> with
            member inline ctx.ChoiceCont (choice : Choice<'T,exn>) =
                match choice with
                | Choice1Of2 t -> ctx.scont t
                | Choice2Of2 e -> ctx.econt e

            member inline ctx.ChoiceCont (choice : Choice<Cloud<'T>, exn>) =
                match choice with
                | Choice1Of2 (Body f) -> f ctx
                | Choice2Of2 e -> ctx.econt e

        let inline ret t = Body(fun ctx -> ctx.scont t)
        let inline raiseM<'T> e : Cloud<'T> = Body(fun ctx -> ctx.econt e)
        let zero = ret ()

        let inline bind (Body f : Cloud<'T>) (g : 'T -> Cloud<'S>) : Cloud<'S> =
            Body(fun ctx ->
                f {
                    Resource = ctx.Resource
                    CancellationToken = ctx.CancellationToken
                    
                    scont = fun t -> ctx.ChoiceCont (protect g t)
                    econt = ctx.econt
                    ccont = ctx.ccont
                }
            )

        let inline combine (f : Cloud<unit>) (g : Cloud<'T>) : Cloud<'T> = bind f (fun () -> g)
        let inline delay (f : unit -> Cloud<'T>) : Cloud<'T> = bind zero f

        let inline tryWith (Body f : Cloud<'T>) (handler : exn -> Cloud<'T>) : Cloud<'T> =
            Body(fun ctx ->
                f {
                    Resource = ctx.Resource
                    CancellationToken = ctx.CancellationToken

                    scont = ctx.scont
                    econt = fun e -> ctx.ChoiceCont (protect handler e)
                    ccont = ctx.ccont
                }
            )

        let inline tryFinally (Body f : Cloud<'T>) (finalizer : unit -> unit) : Cloud<'T> =
            Body(fun ctx ->
                f {
                    Resource = ctx.Resource
                    CancellationToken = ctx.CancellationToken

                    scont = fun t -> match protect finalizer () with Choice1Of2 () -> ctx.scont t | Choice2Of2 e -> ctx.econt e
                    econt = fun e -> match protect finalizer () with Choice1Of2 () -> ctx.econt e | Choice2Of2 e' -> ctx.econt e'
                    ccont = ctx.ccont
                }
            )

        let inline using<'T, 'S when 'T :> ICloudDisposable> (t : 'T) (g : 'T -> Cloud<'S>) : Cloud<'S> =
            Body(fun ctx ->
                let disposer scont =
                    let wf = async { return! t.Dispose () }
                    Async.StartWithContinuations(wf, scont, ctx.econt, ctx.ccont, ctx.CancellationToken)

                match protect g t with
                | Choice1Of2 (Body g) ->
                    g { ctx with 
                            scont = fun s -> disposer (fun () -> ctx.scont s)
                            econt = fun e -> disposer (fun () -> ctx.econt e) }
                | Choice2Of2 e -> disposer (fun () -> ctx.econt e)
            )

        let inline forM (body : 'T -> Cloud<unit>) (ts : 'T []) : Cloud<unit> =
            let rec loop i () =
                if i = ts.Length then ret ()
                else
                    match protect body ts.[i] with
                    | Choice1Of2 b -> bind b (loop (i+1))
                    | Choice2Of2 e -> raiseM e

            delay (loop 0)

        let inline whileM (pred : unit -> bool) (body : Cloud<unit>) : Cloud<unit> =
            let rec loop () =
                match protect pred () with
                | Choice1Of2 true -> bind body loop
                | Choice1Of2 false -> ret ()
                | Choice2Of2 e -> raiseM e

            delay loop

    /// Cloud workflow expression builder
    type CloudBuilder () =
        member __.Return (t : 'T) = ret t
        member __.Zero () = zero
        member __.Delay (f : unit -> Cloud<'T>) = delay f
        member __.ReturnFrom (c : Cloud<'T>) = c
        member __.Combine(f : Cloud<unit>, g : Cloud<'T>) = combine f g
        member __.Bind (f : Cloud<'T>, g : 'T -> Cloud<'S>) : Cloud<'S> = bind f g
        member __.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> = using value bindF

        member __.TryWith(f : Cloud<'T>, handler : exn -> Cloud<'T>) : Cloud<'T> = tryWith f handler
        member __.TryFinally(f : Cloud<'T>, finalizer : unit -> unit) : Cloud<'T> = tryFinally f finalizer

        member __.For(xs : 'T [], body : 'T -> Cloud<unit>) : Cloud<unit> = forM body xs

        [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 444)>]
        member __.While(pred : unit -> bool, body : Cloud<unit>) : Cloud<unit> = whileM pred body

    [<AutoOpen>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module CloudBuilder =
        
        /// cloud builder instance
        let cloud = new CloudBuilder ()