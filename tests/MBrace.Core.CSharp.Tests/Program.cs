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
        public static Cloud<int> Fibonacci(int n)
        {
            return Cloud.New(() =>
                (n <= 1) ?
                    Cloud.FromValue(1) :
                    Cloud.Parallel(
                        Fibonacci(n - 1),
                        Fibonacci(n - 2))
                    .Then(fs => fs.Sum().AsCloud())
            );
        }

        public static Cloud<int> Fib(int n)
        {
            return Cloud.New<int>(() =>
            {
                if (n <= 1)
                    return Cloud.FromValue(1);
                else
                    return
                        from x in Fib(n - 1)
                        from y in Fib(n - 2)
                        select x + y;
            });
        }

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

            //var result1 = rt.Run(Fib(10), null, null);
            var result2 = rt.Run(Fibonacci(10).Computation, null, null);

            rt.KillAllWorkers();
        }
    }
}
