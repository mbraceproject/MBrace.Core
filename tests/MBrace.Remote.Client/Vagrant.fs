module Nessos.MBrace.Remote.Vagrant

open System.Reflection
open System.Collections.Generic

open Nessos.Vagrant
open Nessos.FsPickler
open Nessos.MBrace

let vagrant = Vagrant.Initialize()       

let private ignoredAssemblies = lazy(
    let this = Assembly.GetExecutingAssembly()
    let dependencies = VagrantUtils.ComputeAssemblyDependencies(this)
    new HashSet<_>(dependencies))

type PortableWorkflow =
    {
        Pickle : byte []
        Dependencies : PortableAssembly list
    }
with
    static member Create(workflow : Cloud<unit>) =  
        let assemblyPackages = 
            vagrant.ComputeObjectDependencies(workflow, permitCompilation = true)
            |> List.filter (not << ignoredAssemblies.Value.Contains)
            |> List.map (fun a -> vagrant.CreatePortableAssembly(a, includeAssemblyImage = true))
        {
            Pickle = vagrant.Pickler.Pickle workflow
            Dependencies = assemblyPackages
        }

    static member Load(package : PortableWorkflow) =
        let _ = vagrant.LoadPortableAssemblies(package.Dependencies)
        vagrant.Pickler.UnPickle<Cloud<unit>>(package.Pickle)