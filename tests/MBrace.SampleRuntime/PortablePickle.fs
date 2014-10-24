module internal Nessos.MBrace.SampleRuntime.PortablePickle

open System.IO
open System.Reflection
open System.Collections.Generic

open Nessos.Vagrant

type PortablePickle<'T> = 
    {
        Pickle : byte []
        Dependencies : AssemblyPackage list
    }

type PortablePickle private () =

    static let ignoredAssemblies =
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        new HashSet<_>(dependencies)
    
    static let vagrant = 
        let cachePath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-%O" <| System.Guid.NewGuid())
        let d = Directory.CreateDirectory cachePath
        Vagrant.Initialize(cacheDirectory = cachePath, loadPolicy = AssemblyLoadPolicy.ResolveAll, isIgnoredAssembly = ignoredAssemblies.Contains)

    static member Pickle (value : 'T, ?includeAssemblies) : PortablePickle<'T> =
        let assemblyPackages =
            if defaultArg includeAssemblies true then
                vagrant.ComputeObjectDependencies(value, permitCompilation = true)
                |> List.filter (not << ignoredAssemblies.Contains)
                |> List.map (fun a -> vagrant.CreateAssemblyPackage(a, includeAssemblyImage = true))
            else
                []

        let pickle = vagrant.Pickler.Pickle value

        { Pickle = pickle ; Dependencies = assemblyPackages }

    static member UnPickle(pickle : PortablePickle<'T>) =
        let _ = vagrant.LoadAssemblyPackages(pickle.Dependencies)
        vagrant.Pickler.UnPickle<'T>(pickle.Pickle)