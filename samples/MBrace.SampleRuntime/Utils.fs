namespace MBrace.SampleRuntime

type ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
    static member Empty = new ImmutableQueue<'T>([],[])
    member __.Count = front.Length + back.Length
    member __.IsEmpty = List.isEmpty front && List.isEmpty back
    member __.Enqueue t = new ImmutableQueue<'T>(front, t :: back)
    member __.EnqueueMultiple ts = new ImmutableQueue<'T>(front, List.rev ts @ back)
    member __.TryDequeue () =
        match front with
        | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, back))
        | [] -> 
            match List.rev back with
            | [] -> None
            | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, []))

    member __.ToSeq() = seq { yield! front ; yield! List.rev back }



type QueueTopic<'Topic, 'Msg when 'Topic : comparison> = 
    {
        GlobalQueue : ImmutableQueue<'Msg>
        Topics : Map<'Topic, ImmutableQueue<'Msg>>
    }
with
    static member Empty : QueueTopic<'Topic, 'Msg> = { GlobalQueue = ImmutableQueue.Empty ; Topics = Map.empty }

    member state.Enqueue(topic : 'Topic option, msg : 'Msg) =
        match topic with
        | None -> { state with GlobalQueue = state.GlobalQueue.Enqueue msg }
        | Some t -> 
            match state.Topics.TryFind t with
            | None ->
                let queue = ImmutableQueue.Empty.Enqueue msg
                { state with Topics = state.Topics.Add(t, queue) }
            | Some qr ->
                let qr' = qr.Enqueue msg
                { state with Topics = state.Topics.Add(t, qr') }

    member state.Dequeue(topic : 'Topic) =
        let inline dequeueGlobal() =
            match state.GlobalQueue.TryDequeue() with
            | None -> None
            | Some(msg, gq') -> Some (msg, { state with GlobalQueue = gq' })

        match state.Topics.TryFind topic with
        | None -> dequeueGlobal()
        | Some queue ->
            match queue.TryDequeue() with
            | None -> dequeueGlobal()
            | Some(msg, gq') -> Some(msg, { state with Topics = state.Topics.Add(topic, gq')})

    member state.Cleanup(cleanup : 'Topic -> bool) =
        let pruned = new ResizeArray<'Msg> ()
        let topics =
            state.Topics
            |> Map.filter(fun t q -> 
                            if q.IsEmpty then false
                            elif cleanup t then
                                pruned.AddRange (q.ToSeq())
                                false
                            else
                                true)

        pruned.ToArray(), { state with Topics = topics }