#r "../bin/MBrace.Cloud.dll"

open Nessos.MBrace

let test1 = cloud {
    let! x = cloud { return 27 }
    do printfn "hello!"
    let! y = cloud { try return failwith "boom!" with e -> printfn "Caught exception!" ; return raise e }
    return x + y
}

let rec loop n = cloud {
    do printfn "looping.."
    if n = 100000 then return 0
    else
        let! n = loop (n + 1)
        return n + 1
}

let forLoop = cloud {
    for i in [| 1 .. 10000 |] do
        if i = 555 then return failwith "kaboom"
        printfn "%d" i
}

let whileLoop = cloud {
    let cnt = ref 0
    while !cnt < 10 do
        printfn "%d" !cnt
        if !cnt = 5 then return failwith "kaboom"
        incr cnt 
}

let disposable = 
    { 
        new ICloudDisposable with 
            member __.Dispose () = async { printfn "disposed" }
            member __.GetObjectData (_,_) = ()
    }


let disposableTest = cloud {
    let! x = cloud {
        use x = disposable

//        return failwith "error"
        return printfn "exit scope"
    }
    return printfn "exit"
}

Cloud.RunLocal test1
Cloud.RunLocal (loop 0)
Cloud.RunLocal forLoop
Cloud.RunLocal whileLoop
Cloud.RunLocal disposableTest