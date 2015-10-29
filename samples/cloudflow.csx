#r "../../packages/FSharp.Core/lib/net40/FSharp.Core.dll"
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

var url = "http://publicdata.landregistry.gov.uk/market-trend-data/price-paid-data/a/pp-2015.csv";
var flow = CloudFlow.OfHTTPFileByLine(url)
                    .Select(line => line.Split(','))
                    .Select(arr => new { TransactionId = arr[0], Price = arr[1], City = arr[12] })
                    .Where(trans => trans.City.Contains("LONDON"))
                    .OrderByDescending(trans => trans.Price, 100)
                    .ToArray();


var proc = cluster.CreateProcess(flow);