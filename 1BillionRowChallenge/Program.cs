using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static void Main(string[] args)
    {
        // var summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<IdGeneratorBenchmark>();
        // return;

        IDataStreamProcessorV4 processor = new DataStreamProcessorV5();
        // BenchmarkProcessor(processor, 10, FilePathConstants.Measurements10, CorrectHashes.Measurements10);
        // BenchmarkProcessor(processor, 10_000, FilePathConstants.Measurements10_000, CorrectHashes.Measurements10_000);
        // BenchmarkProcessor(processor, 100_000, FilePathConstants.Measurements100_000, CorrectHashes.Measurements100_000);
        // BenchmarkProcessor(processor, 1_000_000, FilePathConstants.Measurements1_000_000, CorrectHashes.Measurements1_000_000);
        for (int i = 0; i < 10; i++)
        {
            BenchmarkProcessor(processor, 10_000_000, FilePathConstants.Measurements10_000_000, CorrectHashes.Measurements10_000_000);
        }
        // BenchmarkProcessor(processor, 1_000_000_000, FilePathConstants.Measurements1_000_000_000, CorrectHashes.Measurements1_000_000_000);
    }

    private static void BenchmarkProcessor(IDataStreamProcessorV4 processor, long rowCount, string filePath, string correctHash)
    {
        // List<ResultRowV4> processedData = new();
        TimeLogger.LogExecution($"Processing {rowCount:N0} rows using {processor.GetType().Name}", () =>
        {
            processor.ProcessData(filePath);
        }, rowCount);
        // IPresenterV5 presenter = new PresenterV5();
        // string result = presenter.BuildResultString(processedData);
        
        // if (Hasher.Hash(result) == correctHash)
        // {
        //     Console.WriteLine("Correct!");
        // }
        // else
        // {
        //     Console.ForegroundColor = ConsoleColor.Red;
        //     Console.WriteLine("Incorrect!");
        //     Console.WriteLine(Hasher.Hash(result));
        //     
        //     string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{rowCount.ToString("N0").Replace(".", "_")}.out").Trim();
        // }
    }

    private static void PrintHash(string path)
    {
        string fileData = File.ReadAllText(path).Trim();
        string hash = Hasher.Hash(fileData);
        Console.WriteLine(hash);
    }
}