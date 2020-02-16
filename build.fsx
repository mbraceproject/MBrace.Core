// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#r "paket: groupref build //"
#load "./.fake/build.fsx/intellisense.fsx"

open Fake.Core
open Fake.Core.TargetOperators
open Fake.DotNet
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.IO.Globbing.Operators
open Fake.Tools
open Fake.Api
open System

// --------------------------------------------------------------------------------------
// Information about the project to be used at NuGet and in AssemblyInfo files
// --------------------------------------------------------------------------------------

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let gitOwner = "mbraceproject"
let gitName = "MBrace.Core"
let gitHome = "https://github.com/" + gitOwner

let artifactsDir = __SOURCE_DIRECTORY__ @@ "artifacts"

// annoyingly, fake-cli will not load environment variables before module initialization; delay.
let configuration () = Environment.environVarOrDefault "Configuration" "Release" |> DotNet.BuildConfiguration.fromString
let ignoreClusterTests () = Environment.environVarOrDefault "IgnoreClusterTests" "false" |> Boolean.Parse
let ignoreVagabondTests () = Environment.environVarOrDefault "IgnoreVagabondTests" "false" |> Boolean.Parse
let includeCSharpLib () = Environment.environVarOrDefault "IncludeCSharpLib" "false" |> Boolean.Parse

let release = ReleaseNotes.load "RELEASE_NOTES.md"

// --------------------------------------------------------------------------------------
// Clean and restore packages

Target.create "Clean" (fun _ ->
    Shell.cleanDirs [ artifactsDir ]
)

// --------------------------------------------------------------------------------------
// Build

Target.create "Build" (fun _ ->
    DotNet.build (fun opts -> 
        { opts with 
            Configuration = configuration() 

            MSBuildParams =
                { opts.MSBuildParams with
                    Properties = [("Version", release.NugetVersion)] }

        }) __SOURCE_DIRECTORY__
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target.create "RunTests" (fun _ ->
    let testFilter =
        [ if ignoreClusterTests() then yield "TestCategory!=ThespianClusterTests"
          if ignoreVagabondTests() then yield "TestCategory!=ThespianClusterTestsVagabond" ]
        |> String.concat "&"


    DotNet.test (fun c ->
        { c with
            Configuration = configuration()
            Filter = if testFilter = "" then None else Some testFilter
            NoBuild = true
            Blame = true }) __SOURCE_DIRECTORY__
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target.create "NuGet" (fun _ ->    ()
    //Paket.Pack (fun p -> 
    //    { p with 
    //        ToolPath = ".paket/paket.exe" 
    //        OutputPath = "bin/"
    //        Version = nugetVersion
    //        SpecificVersions = ["MBrace.CSharp", release.NugetVersion + "-alpha"]
    //        ReleaseNotes = toLines release.Notes })
)

Target.create "NuGetPush" (fun _ -> ()
    //Paket.Push (fun p -> { p with WorkingDir = "bin/" ; TimeOut = TimeSpan.FromMinutes 30. })
)


// --------------------------------------------------------------------------------------
// Github Releases

Target.create "ReleaseGitHub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.exec "" (sprintf "Bump version to %s" release.NugetVersion)
    Git.Branches.pushBranch "" remote (Git.Information.getBranchName "")

    Git.Branches.tag "" release.NugetVersion
    Git.Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "GITHUB_TOKEN" with
        | null -> 
            let user =
                match Environment.environVarOrDefault "github-user" "" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> UserInput.getUserInput "Username: "
            let pw =
                match Environment.environVarOrDefault "github-pw" "" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> UserInput.getUserInput "Password: "

            GitHub.createClient user pw
        | token -> GitHub.createClientWithToken token

    // release on github
    client
    |> GitHub.draftNewRelease gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes
    |> GitHub.publishDraft
    |> Async.RunSynchronously
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target.create "Default" ignore
Target.create "Release" ignore

"Clean"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "NuGet"
  ==> "NuGetPush"
  ==> "ReleaseGithub"
  ==> "Release"

// start build
Target.runOrDefault "Default"