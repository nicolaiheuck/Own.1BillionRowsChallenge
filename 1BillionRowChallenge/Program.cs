using System.Diagnostics;
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
        // int currentlyZeroNeedsToBeNegativeOne = 0 - 1 | 0b1;
        // int currentlyOneNeedsToBeOne = 1 - 1 | 0b1;
        
        // EnsureDecimalParserWorks();
        // EnsureDecimalParserWorksWithoutDivision();
        // EnsureIntParserWorks();

        // Console.WriteLine("Tests passed");
        
        // DecimalParsingBenchmark.ParseDecimalUsingOwnDecimalParser();
        // Stopwatch stopwatch = Stopwatch.StartNew();
        // BenchmarkDotNet.Running.BenchmarkRunner.Run<DecimalParsingBenchmark>();
        // stopwatch.Stop();
        // Console.WriteLine($"\n\nBenchmark took: {stopwatch.Elapsed.TotalSeconds}s");

        ProgressHelper.Disabled = true;
        IDataStreamProcessorV5 processor = new DataStreamProcessorV7();
        // await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000, 10_000_000));
        // await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements1_000_000_000, 10_000_000));
        // await BenchmarkRunner.BenchmarkRowsPerTask(processor);
        // await BenchmarkRunner.TestAllBelow1BAsync(processor);
        await BenchmarkRunner.Test1B(processor);
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