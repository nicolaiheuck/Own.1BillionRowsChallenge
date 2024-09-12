using System.Diagnostics;
using System.Timers;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    private static System.Timers.Timer? _progressUpdater;
    private static Stopwatch _startOfExecution = new();
    
    public static async Task Main(string[] args)
    {
        // var summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<IdGeneratorBenchmark>();
        // return;

        IDataStreamProcessorV5 processor = new DataStreamProcessorV6();
        // await CalculateProcessingRate(processor);  
        // await BenchmarkRowsPerTask(processor);
        // await TestAllBelow1BAsync(processor);
        await Test1B(processor);

        // const string fileToTest = FilePathConstants.Measurements10_000;
        // foreach (Block block in FileSplitter.SplitFileIntoBlocks(fileToTest, 10))
        // {
        //     BoundaryTester.BoundaryTest(fileToTest, block);
        // }
    }

    private static async Task BenchmarkRowsPerTask(IDataStreamProcessorV5 processor)
    {
        List<int> valuesToTest = [2, 3, 4, 5, 6, 7, 8];
        const int rowCount = 1_000_000_000;
        foreach (int value in valuesToTest)
        {
            long executionTime = await BenchmarkProcessorAsync(processor, rowCount, FilePathConstants.Measurements1_000_000_000, CorrectHashes.Measurements1_000_000_000, value);
            decimal executionTimeInSeconds = executionTime / 1000m;
            decimal rowsPerSecond = Math.Round(rowCount / executionTimeInSeconds / 1_000_000, 2);
            ConsoleHelper.WriteLine($"\r{value:N0} threads = {rowsPerSecond:N2}M                         ");
        }
    }

    private static async Task Test1B(IDataStreamProcessorV5 processor)
    {
        await BenchmarkProcessorAsync(processor, 1_000_000_000, FilePathConstants.Measurements1_000_000_000, CorrectHashes.Measurements1_000_000_000);
    }

    private static ConsoleColor GetColorForState(ThreadProgressState state)
    {
        if (state.IsFinished) return ConsoleColor.Green;
        if (state.LinesProcessedSoFar == 0) return ConsoleColor.DarkGray;
        return ConsoleColor.Yellow;
    }

    private static async Task CalculateProcessingRate(IDataStreamProcessorV5 processor)
    {
        List<decimal> timings = [];
        const int rowCount = 10_000_000;
        for (int i = 0; i < 10; i++)
        {
            long executionTime = await BenchmarkProcessorAsync(processor, rowCount, FilePathConstants.Measurements10_000_000, CorrectHashes.Measurements10_000_000);
            decimal executionTimeInSeconds = executionTime / 1000m;
            int rowsPerSecond = (int)(rowCount / executionTimeInSeconds);
            timings.Add(rowsPerSecond);
        }

        ConsoleHelper.WriteLine($"\nExecution stats: Min: {timings.Min():N0}ms, Max: {timings.Max():N0}ms, Avg: {timings.Average():N0}ms");
    }

    private static async Task TestAllBelow1BAsync(IDataStreamProcessorV5 processor)
    {
        await BenchmarkProcessorAsync(processor, 10, FilePathConstants.Measurements10, CorrectHashes.Measurements10);
        await BenchmarkProcessorAsync(processor, 10_000, FilePathConstants.Measurements10_000, CorrectHashes.Measurements10_000);
        await BenchmarkProcessorAsync(processor, 100_000, FilePathConstants.Measurements100_000, CorrectHashes.Measurements100_000);
        await BenchmarkProcessorAsync(processor, 1_000_000, FilePathConstants.Measurements1_000_000, CorrectHashes.Measurements1_000_000);
        await BenchmarkProcessorAsync(processor, 10_000_000, FilePathConstants.Measurements10_000_000, CorrectHashes.Measurements10_000_000);
    }

    private static async Task<long> BenchmarkProcessorAsync(IDataStreamProcessorV5 processor, long rowCount, string filePath, string correctHash, int? amountOfTasksInTotal = null)
    {
        StartProgressUpdater();
        List<ResultRowV4> processedData = new();
        long executionTime = await TimeLogger.LogExecutionAsync($"Processing {rowCount:N0} rows using {processor.GetType().Name}", async () =>
        {
            _startOfExecution.Start();
            processedData = await processor.ProcessData(filePath, rowCount, amountOfTasksInTotal);
            _startOfExecution.Stop();
        }, rowCount);
        IPresenterV4 presenter = new PresenterV4();
        string result = presenter.BuildResultString(processedData);
        Console.WriteLine("Press a key to continue...");
        Console.ReadKey();
        StopProgressUpdater();
        
        if (Hasher.Hash(result) == correctHash)
        {
            Console.Clear();
            ConsoleHelper.WriteLine("Correct! Press a key to continue.");
            Console.ReadKey();
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            ConsoleHelper.WriteLine("Incorrect!");
            ConsoleHelper.WriteLine(Hasher.Hash(result));
            Console.ResetColor();
            
            string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{rowCount.ToString("N0").Replace(".", "_")}.out").Trim();
            // ConsoleHelper.WriteLine($"You should have:\n{debug}");
            // ConsoleHelper.WriteLine($"You have:\n{result}");
        }

        return executionTime;
    }

    private static void StopProgressUpdater()
    {
        _progressUpdater?.Stop();
        Console.Clear();
    }

    private static void StartProgressUpdater()
    {
        Console.Clear();
        if (_progressUpdater != null) return;
        
        _progressUpdater = new(TimeSpan.FromMilliseconds(500));
        _progressUpdater.Start();
        _progressUpdater.Elapsed += UpdateThreadProgress;
    }

    private static void UpdateThreadProgress(object? sender, ElapsedEventArgs e)
    {
        int i = 0;
        foreach ((Guid taskId, ThreadProgressState? state) in DataStreamProcessorV6.CurrentThreadState.ToList().OrderByDescending(s => s.Value.IsFinished).ThenByDescending(s => s.Value.LinesProcessedSoFar > 0))
        {
            ConsoleColor consoleColor = GetColorForState(state);
            decimal percent = state.LinesProcessedSoFar / (decimal)state.LinesToProcess;
            ConsoleHelper.ColoredWriteLine($"[Thread {taskId}] {percent:P0}      (lines: {state.LinesProcessedSoFar:N0}/{state.LinesToProcess:N0}, " +
                                           $"bytes: {state.BytesReadSoFar:N0}/{state.BytesToRead:N0}, " +
                                           $"rows per sec: {state.LinesProcessedSoFar/state.Stopwatch.Elapsed.TotalSeconds:N0})                 ", consoleColor, 0, i++);
        }

        var rowsPerSec = DataStreamProcessorV6.CurrentThreadState.Values.Sum(s => s.LinesProcessedSoFar) / _startOfExecution.Elapsed.TotalSeconds;
        ConsoleHelper.ColoredWriteLine($"Total rows per sec: {rowsPerSec:N0} (has been running for {_startOfExecution.Elapsed.TotalSeconds:N0}s)", ConsoleColor.Green, 0, i + 1);
    }
}