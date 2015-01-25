namespace MBrace.Tests

open System.Collections.Generic

open MBrace
open MBrace.Continuation

type DummyDisposable() =
    let isDisposed = ref false
    interface ICloudDisposable with
        member __.Dispose () = cloud { isDisposed := true }

    member __.IsDisposed = !isDisposed

type CloudTree<'T> = Leaf | Branch of 'T * TreeRef<'T> * TreeRef<'T>

and TreeRef<'T> = CloudRef<CloudTree<'T>>

module CloudTree =

    let rec createTree d = cloud {
        if d = 0 then return! CloudRef.New Leaf
        else
            let! l,r = createTree (d-1) <||> createTree (d-1)
            return! CloudRef.New (Branch(d, l, r))
    }

    let rec getBranchCount (tree : TreeRef<int>) = cloud {
        let! value = tree.Value
        match value with
        | Leaf -> return 0
        | Branch(_,l,r) ->
            let! c,c' = getBranchCount l <||> getBranchCount r
            return 1 + c + c'
    }

module WordCount =

    let run size mapReduceAlgorithm : Cloud<int> =
        let mapF (text : string) = cloud { return text.Split(' ').Length }
        let reduceF i i' = cloud { return i + i' }
        let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
        mapReduceAlgorithm mapF reduceF 0 inputs

    // naive, binary recursive mapreduce implementation
    let rec mapReduceRec (mapF : 'T -> Cloud<'S>) 
                            (reduceF : 'S -> 'S -> Cloud<'S>) 
                            (id : 'S) (inputs : 'T []) =
        cloud {
            match inputs with
            | [||] -> return id
            | [|t|] -> return! mapF t
            | _ ->
                let left = inputs.[.. inputs.Length / 2 - 1]
                let right = inputs.[inputs.Length / 2 ..]
                let! s,s' = (mapReduceRec mapF reduceF id left) <||> (mapReduceRec mapF reduceF id right)
                return! reduceF s s'
        }


type DisposableRange (start : int, stop : int) =
    let isDisposed = ref false
    let getEnumerator () =
        let count = ref (start - 1)
        let check() = if !isDisposed then raise <| new System.ObjectDisposedException("enumerator")
        {
            new IEnumerator<int> with
                member __.Current = check () ; !count
                member __.Current = check () ; box !count
                member __.MoveNext () =
                    check ()
                    if !count < stop then
                        incr count
                        true
                    else
                        false

                member __.Dispose () = isDisposed := true
                member __.Reset () = raise <| System.NotSupportedException()
        }

    member __.IsDisposed = !isDisposed

    interface seq<int> with
        member __.GetEnumerator() = getEnumerator()
        member __.GetEnumerator() = getEnumerator() :> System.Collections.IEnumerator