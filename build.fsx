// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"

open Fake
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO


let project = "MBrace.Core"
let authors = [ "Eirik Tsarpalis" ; "Nick Palladinos" ; "Kostas Rontogiannis" ]

let description = """Cloud workflow core libraries."""

let tags = "F# cloud mapreduce distributed"

let coreSummary = """
    The MBrace core library contains all cloud workflow essentials,
    libraries and local execution tools for authoring distributed code.
"""

let streamsSummary = """
    MBrace library for distributing streaming computations.
"""

let csharpSummary = """
    MBrace programming model API for C#.
"""

let testsSummary = """
    A collection of abstract NUnit-based test suites for evaluating
    MBrace runtime implementations.
"""

let runtimeSummary = """
    MBrace runtime core library containing the foundations
    for implementing distributed runtimes that support cloud workflows.
"""

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 
let nugetVersion = release.NugetVersion

let gitHome = "https://github.com/mbraceproject"
let gitName = "MBrace.Core"

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [ 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "MBrace"
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
        ]

    !! "./src/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./src/**/AssemblyInfo.cs"
    |> Seq.iter (fun info -> CreateCSharpAssemblyInfo info attributes)
)


// --------------------------------------------------------------------------------------
// Clean and restore packages

Target "Clean" (fun _ ->
    CleanDirs (!! "**/bin/Release/")
    CleanDir "bin/"
)

// --------------------------------------------------------------------------------------
// Build


let configuration = environVarOrDefault "Configuration" "Release"
let ignoreClusterTests = environVarOrDefault "IgnoreClusterTests" "false" |> Boolean.Parse

Target "Build" (fun _ ->
    // Build the rest of the project
    { BaseDirectory = __SOURCE_DIRECTORY__
      Includes = [ project + ".sln" ]
      Excludes = [] } 
    |> MSBuild "" "Build" ["Configuration", configuration]
    |> Log "AppBuild-Output: "
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete


let testAssemblies = 
    [
        yield "bin/MBrace.Core.Tests.dll"
//        yield "bin/MBrace.CSharp.Tests.dll"
        yield "bin/MBrace.Runtime.Core.Tests.dll"
        yield "bin/MBrace.Streams.Tests.dll"
//        yield "bin/MBrace.Streams.CSharp.Tests.dll"
        if not ignoreClusterTests then yield "bin/MBrace.SampleRuntime.Tests.dll"
    ]

Target "RunTests" (fun _ ->
    testAssemblies
    |> NUnit (fun p -> 
        { p with
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 60.
            OutputFile = "TestResults.xml" })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

//// --------------------------------------------------------------------------------------
//// Build a NuGet package

let addFile (target : string) (file : string) =
    if File.Exists (Path.Combine("nuget", file)) then (file, Some target, None)
    else raise <| new FileNotFoundException(file)

let addAssembly reqXml (target : string) assembly =
    let includeFile force file =
        let file = file
        if File.Exists (Path.Combine("nuget", file)) then [(file, Some target, None)]
        elif force then raise <| new FileNotFoundException(file)
        else []

    seq {
        yield! includeFile true assembly
        yield! includeFile reqXml <| Path.ChangeExtension(assembly, "xml")
        yield! includeFile true <| Path.ChangeExtension(assembly, "pdb")
        yield! includeFile false <| assembly + ".config"
    }

Target "NuGet.Core" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Core"
            Summary = coreSummary
            Description = coreSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = []
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Core.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "NuGet.Streams" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Streams"
            Summary = streamsSummary
            Description = streamsSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    "MBrace.Core", RequireExactly nugetVersion
                    "Streams", "0.2.9"
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Streams.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "NuGet.CSharp" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.CSharp"
            Summary = csharpSummary
            Description = csharpSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    ("FSharp.Core", "3.1.2.1")
                    ("MBrace.Core", RequireExactly release.NugetVersion)
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.CSharp.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "NuGet.Streams.CSharp" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Streams.CSharp"
            Summary = streamsSummary
            Description = streamsSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    "MBrace.CSharp", RequireExactly nugetVersion
                    "MBrace.Streams", RequireExactly nugetVersion
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Streams.CSharp.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "NuGet.Runtime.Core" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Runtime.Core"
            Summary = runtimeSummary
            Description = runtimeSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    ("MBrace.Core", RequireExactly release.NugetVersion)
                    ("FsPickler", "1.0.13")
                    ("Vagabond", "0.3.2")
                    ("Unquote", "2.2.2")
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Runtime.Core.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "NuGet.Tests" (fun _ ->
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Tests"
            Summary = testsSummary
            Description = testsSummary
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Dependencies = 
                [
                    ("MBrace.Core", RequireExactly release.NugetVersion)
                    ("MBrace.Streams", RequireExactly release.NugetVersion)
                    ("NUnit", "2.6.3")
                    ("FsCheck", "1.0.4")
                ]
            Publish = hasBuildParam "nugetkey" 
            Files =
                [
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Core.Tests.dll"
                    yield! addAssembly true @"lib\net45" @"..\bin\MBrace.Streams.Tests.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

// --------------------------------------------------------------------------------------
// documentation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    fullclean tempDocsDir
    CopyRecursive "docs/output" tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "Release" DoNothing
Target "NuGet" DoNothing
Target "PrepareRelease" DoNothing
Target "Help" (fun _ -> PrintTargets() )

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "PrepareRelease"
  ==> "NuGet.Core"
  ==> "NuGet.Streams"
//  ==> "NuGet.CSharp" // disable for now
//  ==> "NuGet.Streams.CSharp"
  ==> "NuGet.Tests"
  ==> "NuGet.Runtime.Core"
  ==> "NuGet"
  ==> "Release"

//// start build
RunTargetOrDefault "Default"