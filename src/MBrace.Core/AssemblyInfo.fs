namespace System
open System.Reflection

[<assembly: AssemblyProductAttribute("MBrace")>]
[<assembly: AssemblyCompanyAttribute("Nessos Information Technologies")>]
[<assembly: AssemblyCopyrightAttribute("© Nessos Information Technologies.")>]
[<assembly: AssemblyTrademarkAttribute("MBrace")>]
[<assembly: AssemblyVersionAttribute("1.1.3")>]
[<assembly: AssemblyFileVersionAttribute("1.1.3")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.1.3"
