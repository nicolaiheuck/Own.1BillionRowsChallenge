using System.Text.Json;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;

namespace _1BillionRowChallenge;

public static class BenchmarkRunner
{
    private const int _performanceTolerance = 100_000;
    public static async Task BenchmarkRowsPerTask(IDataStreamProcessorV5 processor)
    {
        List<int> valuesToTest = [2, 3, 4, 5, 6, 7, 8];
        const int rowCount = 1_000_000_000;
        foreach (int value in valuesToTest)
        {
            long executionTime = await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements1_000_000_000, rowCount));
            decimal executionTimeInSeconds = executionTime / 1000m;
            decimal rowsPerSecond = Math.Round(rowCount / executionTimeInSeconds / 1_000_000, 2);
            Console.WriteLine($"\r{value:N0} threads = {rowsPerSecond:N2}M                         ");
        }
    }

    public static async Task Test1B(IDataStreamProcessorV5 processor)
    {
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements1_000_000_000));
    }

    public static async Task CalculateProcessingRate(IDataStreamProcessorV5 processor, BenchmarkConfiguration configuration, int? rowCount = null)
    {
        List<decimal> timings = [];
        for (int i = 0; i < 10; i++)
        {
            Console.Write($"Starting {i + 1}. round...");
            long executionTime = await BenchmarkProcessorAsync(processor, configuration);
            decimal executionTimeInSeconds = executionTime / 1000m;
            int rowsPerSecond = (int)((rowCount ?? configuration.RowCount) / executionTimeInSeconds);
            Console.WriteLine($"\rFinished {i + 1}. round in {executionTime}ms. Rows per second: {rowsPerSecond:N0}");
            timings.Add(rowsPerSecond);
        }

        ProcessingRate processingRate = new()
        {
            Min = timings.Min(),
            Max = timings.Max(),
            Average = timings.Average()
        };

        Console.WriteLine();
        string lastLine = (await File.ReadAllLinesAsync("timings.txt")).Last();
        ProcessingRate lastProcessingRate = JsonSerializer.Deserialize<ProcessingRate>(lastLine)!;

        ConsoleColor color = GetMeasurementColor(processingRate.Average, lastProcessingRate.Average);
        string changeDescription = GetChangeDescription(processingRate, lastProcessingRate);
        Console.WriteLine($"Last measurements: {lastProcessingRate}{changeDescription}");
        ConsoleHelper.ColoredWriteLine($"Execution stats:   {processingRate}", color);
        Console.WriteLine("Debug:");
        Console.WriteLine(await File.ReadAllTextAsync("timings.txt"));
        await File.AppendAllTextAsync("timings.txt", $"{JsonSerializer.Serialize(processingRate)}\n");
    }

    private static string GetChangeDescription(ProcessingRate processingRate, ProcessingRate lastProcessingRate)
    {
        decimal change = processingRate.Average - lastProcessingRate.Average;
        string changeDescription = "";
        if (processingRate.Average - _performanceTolerance > lastProcessingRate.Average)
        {
            changeDescription = $" (Improved by {change:N0} rows/sec ↑)";
        }
        else if (processingRate.Average + _performanceTolerance < lastProcessingRate.Average)
        {
            changeDescription = $" (Decreased by {change:N0} rows/sec ↓)";
        }

        return changeDescription;
    }

    private static ConsoleColor GetMeasurementColor(decimal processingRateAverage, decimal lastProcessingRateAverage)
    {
        ConsoleColor color = ConsoleColor.Black;

        if (processingRateAverage - _performanceTolerance > lastProcessingRateAverage)
        {
            color = ConsoleColor.Green;
        }
        else if (processingRateAverage + _performanceTolerance < lastProcessingRateAverage)
        {
            color = ConsoleColor.Red;
        }

        return color;
    }

    public class ProcessingRate
    {
        public decimal Min { get; set; }
        public decimal Max { get; set; }
        public decimal Average { get; set; }

        public override string ToString()
        {
            return $"Min: {Min:N0}ms, Max: {Max:N0}ms, Avg: {Average:N0}ms";
        }
    }

    public static async Task TestAllBelow1BAsync(IDataStreamProcessorV5 processor)
    {
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10));
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000));
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements100_000));
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements1_000_000));
        await BenchmarkProcessorAsync(processor, BenchmarkConfigurationFactory.Create(BenchmarkType.Measurements10_000_000));
    }

    private static async Task<long> BenchmarkProcessorAsync(IDataStreamProcessorV5 processor, BenchmarkConfiguration configuration)
    {
        ProgressHelper.StartProgressUpdater();
        List<ResultRowV4> processedData = new();
        long executionTime = await TimeLogger.LogExecutionAsync($"Processing {configuration.RowCount:N0} rows using {processor.GetType().Name}", async () =>
        {
            ProgressHelper.StartExecutionTimer();
            processedData = await processor.ProcessData(configuration.FilePath, configuration.RowCount);
            ProgressHelper.StopExecutionTimer();
        }, configuration.RowCount);
        IPresenterV4 presenter = new PresenterV4();
        string result = presenter.BuildResultString(processedData);
        ProgressHelper.StopProgressUpdater();
        
        if (Hasher.Hash(result) == configuration.CorrectHash)
        {
            // Console.Clear();
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Incorrect!");
            Console.WriteLine(Hasher.Hash(result));
            Console.ResetColor();
            
            string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{configuration.RowCount.ToString("N0").Replace(".", "_")}.out").Trim();
            Console.ReadKey();
            // Console.WriteLine($"You should have:\n{debug}");
            // Console.WriteLine($"You have:\n{result}");
        }

        return executionTime;
    }
}