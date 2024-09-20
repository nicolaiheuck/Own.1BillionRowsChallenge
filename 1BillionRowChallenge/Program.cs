using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using _1BillionRowChallenge.Benchmarks;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static async Task Main(string[] args)
    {
        Stopwatch programStopWatch = Stopwatch.StartNew();
        try
        {
            await StartAsync(args);
        }
        finally
        {
            programStopWatch.Stop();
            Console.WriteLine($"\nProgram finished in {programStopWatch.Elapsed.TotalSeconds:N1}s");
        }
    }

    private static async Task StartAsync(string[] args)
    {
        IDataStreamProcessorV5 processor = new DataStreamProcessorV7();
        await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000));
        // await BenchmarkRunner.BenchmarkBestTaskLimit(400, 500, 10, processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000));
        // await BenchmarkRunner.BenchmarkRowsPerTask(processor);
        // await BenchmarkRunner.TestAllBelow1BAsync(processor);
        
        // ProgressHelper.Disabled = true;
        // await BenchmarkRunner.PerformWarmupAsync(processor);
        // await BenchmarkRunner.Test1B(processor);
    }

    private static void EnsureDecimalParserWorksWithoutDivision()
    {
        const string testCase1 = "25.4";
        const string testCase2 = "-12.9";
        const decimal expected1 = 254;
        const decimal expected2 = -129;
        
        decimal result1 = DecimalParsingBenchmark.ParserToTest(testCase1);
        decimal result2 = DecimalParsingBenchmark.ParserToTest(testCase2);
        if (result1 != expected1 || result2 != expected2)
        {
            Console.WriteLine($"Test failed. Expected: {expected1}, {expected2}. Got: {result1}, {result2}");
            // throw new Exception("Test failed");
        }
    }

    private static void EnsureDecimalParserWorks()
    {
        const string testCase1 = "25.4";
        const string testCase2 = "-12.9";
        const decimal expected1 = 25.4m;
        const decimal expected2 = -12.9m;
        
        decimal result1 = DecimalParsingBenchmark.ParserToTest(testCase1);
        decimal result2 = DecimalParsingBenchmark.ParserToTest(testCase2);
        if (result1 != expected1 || result2 != expected2)
        {
            Console.WriteLine($"Test failed. Expected: {expected1}, {expected2}. Got: {result1}, {result2}");
            throw new Exception("Test failed");
        }
    }

    private static void EnsureIntParserWorks()
    {
        const string testCase1 = "12";
        const string testCase2 = "5";
        const int expected1 = 12;
        const int expected2 = 5;
        
        int result1 = IntParserBenchmark.ParserToTest(testCase1);
        int result2 = IntParserBenchmark.ParserToTest(testCase2);
        if (result1 != expected1 || result2 != expected2)
        {
            Console.WriteLine($"Test failed. Expected: {expected1}, {expected2}. Got: {result1}, {result2}");
            throw new Exception("Test failed");
        }
    }
}