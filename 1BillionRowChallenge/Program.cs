using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static async Task Main(string[] args)
    {
        ProgressHelper.Disabled = true;
        IDataStreamProcessorV5 processor = new DataStreamProcessorV7();
        await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000, 10_000_000));
        // await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements1_000_000_000, 10_000_000));
        // await BenchmarkRunner.BenchmarkRowsPerTask(processor);
        // await BenchmarkRunner.TestAllBelow1BAsync(processor);
        // await BenchmarkRunner.Test1B(processor);
    }
}