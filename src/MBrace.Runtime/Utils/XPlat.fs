[<AutoOpen>]
module MBrace.Runtime.Utils.XPlat

open System
open System.Diagnostics
open System.Threading

type Platform =
    | Windows   = 1
    | OSX       = 2
    | Linux     = 4
    | BSD       = 8
    | Unix      = 16
    | Other     = 32

// TODO: refine at a later point
type Runtime =
    | DesktopCLR    = 1
    | CoreCLR       = 2
    | Mono          = 4


/// runs a simple command and returns exit code and stdout
let runCommand (command : string) (args : string) : int * string =
    let psi = new ProcessStartInfo(command, args)
    psi.UseShellExecute <- false
    psi.RedirectStandardOutput <- true
    let proc = Process.Start(psi)
    while not proc.HasExited do Thread.SpinWait 100
    let code = proc.ExitCode
    let output = proc.StandardOutput.ReadToEnd().Trim()
    code, output

/// runs a simple Bourne shell script and returns exit code and stdout
let runBourneShellScript (script : string) =
    runCommand "/bin/sh" <| sprintf "-c '%s'" script

/// gets the platform for the current process
let currentPlatform = lazy(
    match Environment.OSVersion.Platform with
    | PlatformID.MacOSX -> Platform.OSX
    | PlatformID.Unix ->
        try
            let exitCode, output = runBourneShellScript "uname || /bin/uname" // account for docker containers
                                                                              // which do not have '/bin' in $PATH
            if exitCode <> 0 then Platform.Other else

            // c.f. https://en.wikipedia.org/wiki/Uname#Examples
            match output with
            | "Linux" -> Platform.Linux
            | "Darwin" -> Platform.OSX
            | "NetBSD" | "FreeBSD" | "OpenBSD" -> Platform.BSD
            | _ -> Platform.Unix

        with e -> Platform.Other
        
    | _ -> Platform.Windows)

/// gets the current .NET runtime implementation
let currentRuntime = lazy(
    if System.Type.GetType("Mono.Runtime") <> null then Runtime.Mono
    else
        // TODO: CoreCLR support... 
        Runtime.DesktopCLR)

/// Gets the home path for the current user
let getHomePath () =
    match currentPlatform.Value with
    | Platform.Unix | Platform.Linux | Platform.BSD | Platform.OSX -> Environment.GetEnvironmentVariable "HOME"
    | Platform.Windows -> Environment.ExpandEnvironmentVariables "%HOMEDRIVE%%HOMEPATH%"
    | _ -> invalidOp "could not retrieve home path"