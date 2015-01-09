/// Retry utilities
module MBrace.Runtime.Utils.Retry

open System

type RetryPolicy = Policy of (int -> exn -> TimeSpan option)

/// retries given action based on policy
let retry (Policy p) (f : unit -> 'T) =
    let rec aux retries =
        let result = 
            try Choice1Of2 <| f () 
            with e ->
                match p (retries + 1) e with
                | None -> reraise ()
                | Some interval -> Choice2Of2 interval

        match result with
        | Choice1Of2 t -> t
        | Choice2Of2 interval ->
            do System.Threading.Thread.Sleep interval
            aux (retries + 1)

    aux 0

/// retries given action based on given policy
let retryAsync (Policy p) (f : Async<'T>) =
    let rec aux retries =
        async {
            let! result = Async.Catch f

            match result with
            | Choice1Of2 t -> return t
            | Choice2Of2 e ->
                match p (retries + 1) e with
                | None -> return raise e
                | Some interval ->
                    do! Async.Sleep (int interval.TotalMilliseconds)
                    return! aux (retries + 1)
        }

    aux 0


//
//  Predefined retry policies
//

[<Measure>] type sec

let private ofSeconds (seconds : float<sec> option) = 
    match seconds with
    | None -> TimeSpan.Zero
    | Some secs -> TimeSpan.FromSeconds (float secs)

type RetryPolicy with
    /// no retries performed
    static member NoRetry = Policy(fun _ _ -> None)
    /// performs infinitely many retries until operation succeeds
    static member Infinite (?delay : float<sec>) = Policy(fun _ _ -> Some <| ofSeconds delay)
    /// performs given number of retries
    static member Retry(maxRetries : int, ?delay : float<sec>) =
        Policy(fun retries _ ->
            if retries > maxRetries then None
            else Some <| ofSeconds delay)
        
    /// only perform retries while exception of given type is caught
    static member Filter<'exn when 'exn :> Exception>(?delay : float<sec>) =
        Policy(fun _ e -> 
            match e with
            | :? 'exn -> Some <| ofSeconds delay 
            | _ -> None)

    /// doubles the delay interval after every retry
    static member ExponentialDelay(maxRetries : int, initialDelay : float<sec>) =
        Policy(fun retries _ ->
            if retries > maxRetries then None
            else
                Some <| TimeSpan.FromSeconds (float initialDelay * (2.0 ** float (retries - 1))))

    /// maps delay time w.r.t number of performed retries
    static member DelayMap(maxRetries : int, delayF : int -> float<sec>) =
        Policy(fun retries _ ->
            if retries > maxRetries then None
            else
                Some <| TimeSpan.FromSeconds (float (delayF retries)))