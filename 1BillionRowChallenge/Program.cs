using System.Text;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;
using BenchmarkDotNet.Running;

namespace _1BillionRowChallenge;

public class Program
{
    public static void Main(string[] args)
    {
        // var summary = BenchmarkRunner.Run<DataProcessorBenchmark>();
        // PrintHash(@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-1_000_000_000.out");
        TestVersion();
    }

    private static void PrintHash(string path)
    {
        string fileData = File.ReadAllText(path).Trim();
        string hash = Hasher.Hash(fileData);
        Console.WriteLine(hash);
    }

    private static void TestVersion()
    {
        string fileData = File.ReadAllText(FilePathConstants.Measurements10_000_000);
        IDataProcessor processorV1 = new DataProcessorV1();
        List<ResultRow> processedData = processorV1.ProcessData(fileData);
        IPresenter presenter = new PresenterV1();
        string result = presenter.BuildResultString(processedData);
        
        if (Hasher.Hash(result) == CorrectHashes.Measurements10_000_000)
        {
            Console.WriteLine("Correct!");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Incorrect!");
            Console.WriteLine(Hasher.Hash(result));
            
            string debug = File.ReadAllText(@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-1_000_000_000.out").Trim();
            
            string debugAsHex = string.Join("", Encoding.UTF8.GetBytes(debug).Select(b => b.ToString("x2")));
            string resultAsHex = string.Join("", Encoding.UTF8.GetBytes(result).Select(b => b.ToString("x2")));
            // Console.WriteLine(debug);
        }
    }
}