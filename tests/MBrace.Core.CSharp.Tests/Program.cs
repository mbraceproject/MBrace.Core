using Microsoft.FSharp.Core;
using Nessos.MBrace.Core.CSharp;
using Nessos.MBrace.SampleRuntime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Core.CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            var wf =
                Cloud.New(() =>
                    Cloud.Parallel(
                        Cloud.FromValue(20),
                        Cloud.FromValue(22))
                    .Then(ys => ys.Sum().AsCloud())
                );

            MBraceRuntime.WorkerExecutable = Path.Combine(Directory.GetCurrentDirectory(), "MBrace.SampleRuntime.exe");
            var rt = MBraceRuntime.InitLocal(3, null);

            var result = rt.Run(wf, null, null);

            rt.KillAllWorkers();
        }
    }
}
