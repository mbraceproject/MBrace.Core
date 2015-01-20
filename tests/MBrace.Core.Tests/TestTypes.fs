namespace MBrace.Tests

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
        mapReduceAlgorithm mapF 0 reduceF inputs