using MBrace.CSharp;
using MBrace.SampleRuntime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp.Tests
{
    class Program
    {
        public static Cloud<int> Fibonacci(int n)
        {
            return Cloud.New(() =>
            {
                if (n <= 1)
                    return Cloud.FromValue(1);
                else
                    return Cloud.Parallel(
                                Fibonacci(n - 1),
                                Fibonacci(n - 2))
                            .Then(fs => fs.Sum());
            });
        }

        public static Cloud<Option<int>> ChoiceExperiment(int i, int j)
        {
            return Cloud.New(() =>
            {
                Console.WriteLine("i = {0}, j = {1}", i, j);

                if (i == 4 && j == 0) return Option<int>.Some(42).AsCloud();
                else
                    return Cloud.Choice(
					new [] {
                            ChoiceExperiment(i + 1, 0),
                            ChoiceExperiment(i + 1, 1),
                            ChoiceExperiment(i + 1, 2)});
            });
        }

        // This is wrong.
        public static Cloud<Option<int>> ChoiceExperiment2(int i, int j)
        {
            Console.WriteLine("i = {0}, j = {1}", i, j);

            if (i == 4 && j == 0) return Option<int>.Some(42).AsCloud();
            else
                return Cloud.Choice(
				    new [] {
				    ChoiceExperiment2(i + 1, 0),
                        ChoiceExperiment2(i + 1, 1),
				    ChoiceExperiment2(i + 1, 2)});
            
        }

        static void Main(string[] args)
        {
            var wf =
                Cloud.New(() =>
                    Cloud.Parallel(
                        20.AsCloud(),
                        22.AsCloud())
                    .Then(ys => ys.Sum().AsCloud())
                );

            MBraceRuntime.WorkerExecutable = Path.Combine(Directory.GetCurrentDirectory(), "MBrace.SampleRuntime.exe");
            var rt = MBraceRuntime.InitLocal(3);

            var w = Cloud.New(() =>
                Cloud.Choice(
                    Cloud.Sleep(2000).Then(() => 1),
                    Cloud.Sleep(1000).Then(() => 2))
                );
            var x = rt.Run(w.Computation, null, null);

            //var result1 = rt.Run(Fib(10), null, null);
            //var result2 = rt.Run(ChoiceExperiment(0, 0).Computation, null, null);
            //var result3 = rt.Run(Cloud.New(() => ChoiceExperiment(0, 0)).Computation, null, null);

            rt.KillAllWorkers();
        }
    }
}
