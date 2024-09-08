using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static void Main(string[] args)
    {
        // var summary = BenchmarkRunner.Run<DataProcessorBenchmark>();
        TestVersion();
    }

    private static void TestVersion()
    {
        string fileData = File.ReadAllText(FilePathConstants.Measurements10);
        IDataProcessor processorV1 = new DataProcessorV1();
        List<ResultRow> processedData = processorV1.ProcessData(fileData);
        IPresenter presenter = new PresenterV1();
        string result = presenter.BuildResultString(processedData);

        if (Hasher.Hash(result) == CorrectHashes.Measurements10)
        {
            Console.WriteLine("Correct!");
        }
    }
}