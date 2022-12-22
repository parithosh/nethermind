// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using System.Linq;
using BenchmarkDotNet.Filters;

namespace Nethermind.Benchmark.Runner
{
    public class DashboardConfig : ManualConfig
    {
        public DashboardConfig(params Job[] jobs)
        {
            foreach (Job job in jobs)
            {
                AddJob(job);
            }

            AddColumnProvider(BenchmarkDotNet.Columns.DefaultColumnProviders.Statistics);
            AddColumnProvider(BenchmarkDotNet.Columns.DefaultColumnProviders.Params);
            AddLogger(BenchmarkDotNet.Loggers.ConsoleLogger.Default);
            AddExporter(BenchmarkDotNet.Exporters.Json.JsonExporter.FullCompressed);
            AddDiagnoser(BenchmarkDotNet.Diagnosers.MemoryDiagnoser.Default);
            WithSummaryStyle(SummaryStyle.Default.WithMaxParameterColumnWidth(100));
        }
    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            DashboardConfig config = new DashboardConfig(Job.MediumRun.WithRuntime(CoreRuntime.Core60));
            if (args.Length > 0)
            {
                IFilter[] filters = args.Select(a => new SimpleFilter(c => c.Parameters.Items.Any(p=>p.Value.ToString().Contains(a)))).OfType<IFilter>().ToArray();
                config.AddFilter(new DisjunctionFilter(filters));
            }
            BenchmarkRunner.Run(typeof(Precompiles.Benchmark.PointEvaluationBenchmark), config);
            BenchmarkRunner.Run(typeof(Precompiles.Benchmark.EcRecoverBenchmark), config);
            // List<Assembly> additionalJobAssemblies = new()
            // {
            //     typeof(Nethermind.JsonRpc.Benchmark.EthModuleBenchmarks).Assembly,
            //     typeof(Nethermind.Benchmarks.Core.Keccak256Benchmarks).Assembly,
            //     typeof(Nethermind.Evm.Benchmark.EvmStackBenchmarks).Assembly,
            //     typeof(Nethermind.Network.Benchmarks.DiscoveryBenchmarks).Assembly,
            //     typeof(Nethermind.Precompiles.Benchmark.KeccakBenchmark).Assembly
            // };
            //
            // List<Assembly> simpleJobAssemblies = new()
            // {
            //     typeof(Nethermind.EthereumTests.Benchmark.EthereumTests).Assembly,
            // };
            //
            // if (Debugger.IsAttached)
            // {
            //     BenchmarkSwitcher.FromAssemblies(additionalJobAssemblies.Union(simpleJobAssemblies).ToArray()).RunAll(new DebugInProcessConfig());
            // }
            // else
            // {
            //     foreach (Assembly assembly in additionalJobAssemblies)
            //     {
            //         BenchmarkRunner.Run(assembly, new DashboardConfig(Job.MediumRun.WithRuntime(CoreRuntime.Core60)));
            //     }
            //
            //     foreach (Assembly assembly in simpleJobAssemblies)
            //     {
            //         BenchmarkRunner.Run(assembly, new DashboardConfig());
            //     }
            // }
        }
    }
}
