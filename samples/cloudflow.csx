#r "../../packages/FSharp.Core/lib/net40/FSharp.Core.dll"
#r "../../packages/test/System.Runtime.Loader/lib/DNXCore50/System.Runtime.Loader.dll"
#r "../../bin/FsPickler.dll"
#r "../../bin/Vagabond.dll"
#r "../../bin/Argu.dll"
#r "../../bin/Newtonsoft.Json.dll"
#r "../../bin/MBrace.Core.dll"
#r "../../bin/MBrace.Runtime.dll"
#r "../../bin/MBrace.Thespian.dll"
#r "../../bin/MBrace.Flow.dll"
#r "../../bin/MBrace.CSharp.dll"
#r "../../bin/Streams.dll"

// before running sample, don't forget to set binding redirects to FSharp.Core in InteractiveHost.exe

using System;
using System.Linq;
using MBrace.Core;
using MBrace.Core.CSharp;
using MBrace.Flow.CSharp;
using MBrace.Library;
using MBrace.Thespian;

ThespianWorker.LocalExecutable = "../../bin/mbrace.thespian.worker.exe";
var logger = (MBrace.Runtime.ISystemLogger)new MBrace.Runtime.ConsoleLogger();
var cluster = ThespianCluster.InitOnCurrentMachine(workerCount: 4, logger: logger.ToOption());

// 1. Hello, World
var getMachineName = CloudBuilder.FromFunc(() => "Hello, World");
cluster.Run(getMachineName);

cluster.ShowProcesses();

// 2. Parallel workflow
var pworkflow =
    Enumerable.Range(1, 10000000)
        .ParallelMap(x => (2 * x + 1) % 100)
        .OnSuccess(xs => xs.Sum());

cluster.Run(pworkflow);

// 3. CloudFlow.CSharp tests
var url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2015.csv";

string trim(string input) { return input.Trim(new char[] { '\"' }); }

var cacheF = CloudFlow.OfHttpFileByLine(url)
                    .Select(line => line.Split(','))
                    .Select(arr => new { TransactionId = Guid.Parse(trim(arr[0])), Price = Double.Parse(trim(arr[1])), City = trim(arr[12]) })
                    .Cache();

var cacheFlowProc = cluster.CreateProcess(cacheF); // Start caching process

cacheFlowProc.ShowInfo();

var cachedFlow = cacheFlowProc.Result; // get the cached CloudFlow

var top10London =
    cachedFlow
        .Where(trans => trans.City.ToLower() == "london")
        .OrderByDescending(trans => trans.Price, 10)
        .ToArray();

cluster.Run(top10London);

var maxAverageCity =
    cachedFlow
        .GroupBy(trans => trans.City.ToLower())
        .Select(gp => new { City = gp.Item1, Average = gp.Item2.Select(t => t.Price).Average() }) 
        .MaxBy(city => city.Average);

cluster.Run(maxAverageCity);