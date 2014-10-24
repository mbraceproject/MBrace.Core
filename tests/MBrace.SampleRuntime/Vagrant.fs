module internal Nessos.MBrace.SampleRuntime.Vagrant

open System.IO
open System.Reflection
open System.Collections.Generic

open Nessos.Vagrant
open Nessos.Thespian
open Nessos.MBrace.SampleRuntime.Actors

// init vagrant state

let initVagrant () =
    // assemblies that the runtime depends on, will be ignored by vagrant
    let ignoredAssemblies =
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        new HashSet<_>(dependencies)

    // set up a unique assembly cache path
    let cachePath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-%O" <| System.Guid.NewGuid())
    let d = Directory.CreateDirectory cachePath
    Vagrant.Initialize(cacheDirectory = cachePath, loadPolicy = AssemblyLoadPolicy.ResolveAll, isIgnoredAssembly = fun a -> ignoredAssemblies.Contains a)

let vagrant = initVagrant()
let pickler = vagrant.Pickler

// assembly exporter

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<AssemblyPackage list> 

type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    member __.RequestAssemblies ids = exporter <!- fun ch -> RequestAssemblies(ids, ch)
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let packages = vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            do! ch.Reply packages
        }

        let ref = 
            Behavior.stateless behaviour 
            |> Actor.bind 
            |> Actor.Publish

        new AssemblyExporter(ref)

// Portable pickles

type PortablePickle<'T> =
    {
        Pickle : byte []
        Dependencies : AssemblyId list
    }

module PortablePickle =

    let pickle (value : 'T) : PortablePickle<'T> =
        let dependencies = 
            vagrant.ComputeObjectDependencies(value, permitCompilation = true)
            |> List.map Utilities.ComputeAssemblyId
        
        let pickle = pickler.Pickle value
        { Pickle = pickle ; Dependencies = dependencies }

    let unpickle (exporter : AssemblyExporter) (pickle : PortablePickle<'T>) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return pickle.Dependencies }
                    member __.PullAssemblies ids = exporter.RequestAssemblies ids
            }

        do! vagrant.ReceiveDependencies publisher

        return pickler.UnPickle<'T> pickle.Pickle
    }