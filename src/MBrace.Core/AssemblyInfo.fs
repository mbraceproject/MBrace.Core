namespace System
open System.Reflection

[<assembly: AssemblyProductAttribute("MBrace")>]
[<assembly: AssemblyCompanyAttribute("Nessos Information Technologies")>]
[<assembly: AssemblyCopyrightAttribute("© Nessos Information Technologies.")>]
[<assembly: AssemblyTrademarkAttribute("MBrace")>]
[<assembly: AssemblyVersionAttribute("1.2.4")>]
[<assembly: AssemblyFileVersionAttribute("1.2.4")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.2.4"
    let [<Literal>] InformationalVersion = "1.2.4"
