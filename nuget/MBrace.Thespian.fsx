#I __SOURCE_DIRECTORY__
#r "tools/Newtonsoft.Json.dll"
#r "tools/FsPickler.dll"
#r "tools/FsPickler.Json.dll"
#r "tools/Mono.Cecil.dll"
#r "tools/Vagabond.AssemblyParser.dll"
#r "tools/Vagabond.dll"
#r "tools/Thespian.dll"
#r "tools/Argu.dll"
#r "tools/MBrace.Core.dll"
#r "tools/MBrace.Runtime.dll"
#r "tools/MBrace.Thespian.dll"

open System.IO
open MBrace.Thespian

ThespianWorker.LocalExecutable <- Path.Combine(__SOURCE_DIRECTORY__, @"tools/mbrace.thespian.worker")
