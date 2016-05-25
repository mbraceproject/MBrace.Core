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

/// gets the platform for the current process
let currentPlatform = lazy(
    match Environment.OSVersion.Platform with
    | PlatformID.MacOSX -> Platform.OSX
    | PlatformID.Unix ->
        let psi = new ProcessStartInfo("uname")
        psi.UseShellExecute <- false
        psi.RedirectStandardOutput <- true
        try
            let proc = Process.Start psi
            while not proc.HasExited do Thread.SpinWait 100
            if proc.ExitCode <> 0 then Platform.Other else
            let output = proc.StandardOutput.ReadToEnd().Trim()
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