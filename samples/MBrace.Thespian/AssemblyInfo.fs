namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace.Core")>]
[<assembly: AssemblyProductAttribute("MBrace.Core")>]
[<assembly: AssemblyCompanyAttribute("Nessos Information Technologies")>]
[<assembly: AssemblyCopyrightAttribute("© Nessos Information Technologies.")>]
[<assembly: AssemblyTrademarkAttribute("MBrace")>]
[<assembly: AssemblyVersionAttribute("0.10.0")>]
[<assembly: AssemblyFileVersionAttribute("0.10.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.10.0"
