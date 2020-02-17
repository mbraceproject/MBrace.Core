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

[<Flags>]
type TestCoverage =
    /// Unit tests only
    | Unit = 1
    /// Run a subset of acceptance tests for smoke testing
    | Smoke = 2
    /// Run the full acceptance test suite
    | Acceptance = 4
    /// Run everything
    | All = 7

module TestCoverage =
    let parse(value : string) = 
        let ok, tc = Enum.TryParse<TestCoverage>(value, ignoreCase = true)
        if ok then tc else invalidArg value "invalid test coverage value"


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
let getTestCoverage () = Environment.environVarOrDefault "TestCoverage" "all" |> TestCoverage.parse

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
    let testCoverage = getTestCoverage()
    let testFilter =
        [ if not <| testCoverage.HasFlag TestCoverage.Smoke then yield "TestCategory!=SmokeTests"
          if not <| testCoverage.HasFlag TestCoverage.Acceptance then yield "TestCategory!=AcceptanceTests" ]
        |> String.concat "&"

    DotNet.test (fun c ->
        { c with
            Configuration = configuration()
            Filter = if testFilter = "" then None else Some testFilter
            ResultsDirectory = Some "TestResults/"
            NoBuild = true
            Blame = true }) __SOURCE_DIRECTORY__
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target.create "NuGet.Pack" (fun _ ->
    let releaseNotes = String.toLines release.Notes |> System.Net.WebUtility.HtmlEncode
    DotNet.pack (fun pack ->
        { pack with
            OutputPath = Some artifactsDir
            Configuration = DotNet.BuildConfiguration.Release
            MSBuildParams =
                { pack.MSBuildParams with
                    Properties = 
                        [("Version", release.NugetVersion)
                         ("PackageReleaseNotes", releaseNotes)] }
        }) __SOURCE_DIRECTORY__
)

Target.create "NuGet.ValidateSourceLink" (fun _ ->
    for nupkg in !! (artifactsDir @@ "*.nupkg") do
        let p = DotNet.exec id "sourcelink" (sprintf "test %s" nupkg)
        if not p.OK then failwithf "failed to validate sourcelink for %s" nupkg
)

Target.create "NuGet.Push" (fun _ ->
    for artifact in !! (artifactsDir + "/*nupkg") do
        let source = "https://api.nuget.org/v3/index.json"
        let key = Environment.GetEnvironmentVariable "NUGET_KEY"
        let result = DotNet.exec id "nuget" (sprintf "push -s %s -k %s %s" source key artifact)
        if not result.OK then failwith "failed to push packages"
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
  ==> "NuGet.Pack"
  //==> "NuGet.ValidateSourceLink"
  ==> "Default"

"Default"
  ==> "NuGet.Push"
  ==> "ReleaseGithub"
  ==> "Release"

// start build
Target.runOrDefault "Default"