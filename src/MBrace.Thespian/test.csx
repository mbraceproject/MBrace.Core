#r "../../bin/FSharp.Core.dll"
#r "../../bin/FsPickler.dll"
#r "../../bin/Vagabond.dll"
#r "../../bin/Argu.dll"
#r "../../bin/Newtonsoft.Json.dll"
#r "../../bin/MBrace.Core.dll"
#r "../../bin/MBrace.Runtime.dll"
#r "../../bin/MBrace.Thespian.dll"
#r "../../bin/MBrace.Flow.dll"
#r "../../bin/MBrace.Flow.CSharp.dll"
#r "../../bin/Streams.dll"


// before running sample, don't forget to set binding redirects to FSharp.Core in InteractiveHost.exe

using System;
using MBrace.Core;
using MBrace.Library;
using MBrace.Thespian;
using MBrace.Flow.CSharp;


ThespianWorker.LocalExecutable = "../../bin/mbrace.thespian.worker.exe";
var cluster = ThespianCluster.InitOnCurrentMachine(4);

var flow = CloudFlow.OfArray(new[] { 1, 2, 3 })
                    .Select(x => x + 1)
                    .Where(x => x % 2 == 0)
                    .ToArray();


var result = cluster.Run(flow);




