using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;
using Microsoft.Diagnostics.Runtime.DacInterface;

namespace _1BillionRowChallenge;

public class Program
{
    public static async Task Main(string[] args)
    {
        // var summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<IdGeneratorBenchmark>();
        // return;

        IDataStreamProcessorV5 processor = new DataStreamProcessorV5();
        // CalculateProcessingRate(processor);
        await TestAllBelow1BAsync(processor);
        // Test1B(processor);
    }

    private static async Task Test1B(IDataStreamProcessorV5 processor)
    {
        await BenchmarkProcessorAsync(processor, 1_000_000_000, FilePathConstants.Measurements1_000_000_000, CorrectHashes.Measurements1_000_000_000);
    }

    private static async Task CalculateProcessingRate(IDataStreamProcessorV5 processor)
    {
        for (int i = 0; i < 10; i++)
        {
            await BenchmarkProcessorAsync(processor, 10_000_000, FilePathConstants.Measurements10_000_000, CorrectHashes.Measurements10_000_000);
        }
    }

    private static async Task TestAllBelow1BAsync(IDataStreamProcessorV5 processor)
    {
        // await BenchmarkProcessorAsync(processor, 10, FilePathConstants.Measurements10, CorrectHashes.Measurements10);
        // await BenchmarkProcessorAsync(processor, 20, FilePathConstants.Measurements20, CorrectHashes.Measurements20);
        // await BenchmarkProcessorAsync(processor, 10_000, FilePathConstants.Measurements10_000, CorrectHashes.Measurements10_000);
        // await BenchmarkProcessorAsync(processor, 100_000, FilePathConstants.Measurements100_000, CorrectHashes.Measurements100_000);
        // await BenchmarkProcessorAsync(processor, 1_000_000, FilePathConstants.Measurements1_000_000, CorrectHashes.Measurements1_000_000);
        await BenchmarkProcessorAsync(processor, 10_000_000, FilePathConstants.Measurements10_000_000, CorrectHashes.Measurements10_000_000);
    }

    private static async Task BenchmarkProcessorAsync(IDataStreamProcessorV5 processor, long rowCount, string filePath, string correctHash)
    {
        List<ResultRowV4> processedData = new();
        await TimeLogger.LogExecutionAsync($"Processing {rowCount:N0} rows using {processor.GetType().Name}", async () =>
        {
            processedData = await processor.ProcessData(filePath, rowCount);
        }, rowCount);
        IPresenterV4 presenter = new PresenterV4();
        string result = presenter.BuildResultString(processedData);
        
        if (Hasher.Hash(result) == correctHash)
        {
            Console.WriteLine("Correct!");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Incorrect!");
            Console.WriteLine(Hasher.Hash(result));
            
            string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{rowCount.ToString("N0").Replace(".", "_")}.out").Trim();
            Console.WriteLine($"You should have:\n{debug}");
            Console.WriteLine($"You have:\n{result}");
        }
    }

    // private static void BenchmarkProcessor(IDataStreamProcessorV4 processor, long rowCount, string filePath, string correctHash)
    // {
    //     List<ResultRowV4> processedData = new();
    //     TimeLogger.LogExecution($"Processing {rowCount:N0} rows using {processor.GetType().Name}", () =>
    //     {
    //         processedData = processor.ProcessData(filePath);
    //     }, rowCount);
    //     IPresenterV5 presenter = new PresenterV5();
    //     string result = presenter.BuildResultString(processedData);
    //     
    //     if (Hasher.Hash(result) == correctHash)
    //     {
    //         Console.WriteLine("Correct!");
    //     }
    //     else
    //     {
    //         Console.ForegroundColor = ConsoleColor.Red;
    //         Console.WriteLine("Incorrect!");
    //         Console.WriteLine(Hasher.Hash(result));
    //         
    //         string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{rowCount.ToString("N0").Replace(".", "_")}.out").Trim();
    //         Console.WriteLine($"You should have:\n{debug}");
    //         Console.WriteLine($"You have:\n{result}");
    //     }
    // }

    private static void PrintHash(string path)
    {
        string fileData = File.ReadAllText(path).Trim();
        string hash = Hasher.Hash(fileData);
        Console.WriteLine(hash);
    }
}