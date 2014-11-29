module Nessos.MBrace.Store.Tests.TestTypes

open Nessos.MBrace

type CloudTree<'T> = Leaf | Branch of 'T * TreeRef<'T> * TreeRef<'T>

and TreeRef<'T> = CloudRef<CloudTree<'T>>

let rec createTree d = cloud {
    if d = 0 then return! CloudRef.New Leaf
    else
        let! l,r = createTree (d-1) <||> createTree (d-1)
        return! CloudRef.New (Branch(d, l, r))
}

let rec getBranchCount (tree : TreeRef<int>) = cloud {
    match tree.Value with
    | Leaf -> return 0
    | Branch(_,l,r) ->
        let! c,c' = getBranchCount l <||> getBranchCount r
        return 1 + c + c'
}