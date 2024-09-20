using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;
using _1BillionRowChallenge.Benchmarks;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public struct TestBlock
{
    public long Start;
    public long End;
}

public class Program
{
    private static int _chunkSize;
    private static long _i;
    private static Stopwatch _stopwatch = null!;

    public static async Task Main(string[] args)
    {
        /*
        _i = 0;
        
        // With 9 tasks
        // const int amountOfChunks = 1_000; // 32.7M
        // const int amountOfChunks = 100_000; // 30M
        // const int amountOfChunks = 10_000; // 31.8M
        // const int amountOfChunks = 1_000_000; // 24.9M
        
        // With unlimited tasks
        // const int amountOfChunks = 500; // 32.7M 
        // const int amountOfChunks = 1_000; // 35.4M (with unlimited tasks)
        // const int amountOfChunks = 10_000; // 33.3 M

        
        // const int chunkSize = 10_000_000; // 32.9M
        // const int chunkSize = 1_000_000; // 32.9M
        // const int chunkSize = 100_000; // 31.3M
        // const int chunkSize = 10_000; // 30.3M
        // const int chunkSize = 1_000; // 26.5M

        // const int distributeOnThreads = 5;
        
        // const int chunkSize = 10_000; // 28.9M
        // const int chunkSize = 100_000; // 31.2M
        // const int chunkSize = 1_000_000; // 30.8M

        const int distributeOnThreads = 10;
        
        // const int chunkSize = 10_000; // 34.0M
        // const int chunkSize = 100_000; // 34.7M
        // const int chunkSize = 1_000_000; // 34.7M
        // const int chunkSize = 10_000_000; // 34.2M
        
        // With BufferedStream
        // _chunkSize = 10_000; //  29.1M
        _chunkSize = 200_000; // 35.8M (with TestBlock)
        // _chunkSize = 1_000_000; // 34.8M
        // _chunkSize = 10_000_000; // 31.9M
        
        
        List<TestBlock> blocks = FileSplitter.SplitFileIntoTestBlocks(FilePathConstants.Measurements1_000_000_000, 1_000_000_000 / _chunkSize);
        using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(FilePathConstants.Measurements1_000_000_000);

        int blocksPerThread = blocks.Count / distributeOnThreads;
        List<TestBlock[]> chunksToProcess = blocks.Chunk(blocksPerThread).ToList();
        
        _stopwatch = Stopwatch.StartNew();
        List<Task> tasks = new();
        foreach (TestBlock[] chunk in chunksToProcess)
        {
            tasks.Add(Task.Run(() => ProcessChunk(memoryMappedFile, chunk)));
        }
        
        Task.WaitAll(tasks.ToArray());
        */

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

    private static Task ProcessChunk(MemoryMappedFile memoryMappedFile, TestBlock[] chunk)
    {
        foreach (TestBlock block in chunk)
        {
            MemoryMappedViewStream viewStream = memoryMappedFile.CreateViewStream(block.Start, block.End - block.Start, MemoryMappedFileAccess.Read);
            BufferedStream bufferedStream = new(viewStream, (int)(block.End - block.Start));
            StreamReader reader = new(bufferedStream);;
            string? line;
            do
            {
                Interlocked.Increment(ref _i);
            
                if (_i % 10_000_000 == 0)
                {
                    Console.Write($"\rProcessed {_i:N0} ({_i / _stopwatch.Elapsed.TotalSeconds:N0} rows/sec)");
                }
            
                line = reader.ReadLine();
            } while (line != null);
        }

        return Task.CompletedTask;
    }

    private static async Task StartAsync(string[] args)
    {
        IDataStreamProcessorV5 processor = new DataStreamProcessorV8();
        // await BenchmarkRunner.CalculateProcessingRate(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000));
        // await BenchmarkRunner.BenchmarkBestTaskLimit(400, 500, 10, processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000));
        // await BenchmarkRunner.BenchmarkRowsPerTask(processor);
        // await BenchmarkRunner.TestAllBelow1BAsync(processor);
        
        ProgressHelper.Disabled = true;
        await BenchmarkRunner.PerformWarmupAsync(processor);
        await BenchmarkRunner.PerformWarmupAsync(processor);
        await BenchmarkRunner.PerformWarmupAsync(processor);
        ProgressHelper.Disabled = false;
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