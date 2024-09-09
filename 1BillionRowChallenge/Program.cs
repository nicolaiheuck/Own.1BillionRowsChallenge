using System.Text;
using _1BillionRowChallenge.Benchmarks;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static void Main(string[] args)
    {
        // var summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<IdGeneratorBenchmark>();
        // PrintHash(@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-1_000_000_000.out");
        // TestVersion();
        // int i = 0;
        // var cityNames = File.ReadAllLines(FilePathConstants.Measurements100_000)
        //                         .Select(l => l.Split(";")[0])
        //                         .DistinctBy(l => l)
        //                         .Select(l =>
        //                         {
        //                             i++;
        //                             return DataStreamProcessorV5.GenerateIdFromCityName(l, i);
        //                         })
        //                         .Distinct()
        //                         .ToList();
        //
        // Console.WriteLine("cityNames: " + cityNames.Count);
        // return;

        IDataStreamProcessorV5 processor = new DataStreamProcessorV5();
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

    private static void BenchmarkProcessor(IDataStreamProcessorV5 processor, long rowCount, string filePath, string correctHash)
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

    // private static void TestVersion()
    // { 
    //     string fileData = File.ReadAllText(FilePathConstants.Measurements1_000_000_000);
    //     IDataProcessor processorV1 = new DataProcessorV1();
    //     List<ResultRow> processedData = processorV1.ProcessData(fileData);
    //     IPresenter presenter = new PresenterV1();
    //     string result = presenter.BuildResultString(processedData);
    //     
    //     if (Hasher.Hash(result) == CorrectHashes.Measurements1_000_000_000)
    //     {
    //         Console.WriteLine("Correct!");
    //     }
    //     else
    //     {
    //         Console.ForegroundColor = ConsoleColor.Red;
    //         Console.WriteLine("Incorrect!");
    //         Console.WriteLine(Hasher.Hash(result));
    //         
    //         string debug = File.ReadAllText(@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-1_000_000_000.out").Trim();
    //         
    //         string debugAsHex = string.Join("", Encoding.UTF8.GetBytes(debug).Select(b => b.ToString("x2")));
    //         string resultAsHex = string.Join("", Encoding.UTF8.GetBytes(result).Select(b => b.ToString("x2")));
    //         // Console.WriteLine(debug);
    //     }
    // }
}