﻿using System.Timers;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge;

public class Program
{
    public static async Task Main(string[] args)
    {
        // var summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<IdGeneratorBenchmark>();
        // return;

        IDataStreamProcessorV5 processor = new DataStreamProcessorV6();
        // await CalculateProcessingRate(processor);
        // await BenchmarkRowsPerTask(processor);
        // await TestAllBelow1BAsync(processor);
        await Test1B(processor);
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
            ConcurrentConsoleHelperDecorator.WriteLine($"\r{value:N0} threads = {rowsPerSecond:N2}M                         ");
        }
    }

    private static async Task Test1B(IDataStreamProcessorV5 processor)
    {
        System.Timers.Timer progressUpdater = new(TimeSpan.FromMilliseconds(500));
        progressUpdater.Start();
        progressUpdater.Elapsed += UpdateThreadProgress;
        await BenchmarkProcessorAsync(processor, 1_000_000_000, FilePathConstants.Measurements1_000_000_000, CorrectHashes.Measurements1_000_000_000);
    }

    private static void UpdateThreadProgress(object? sender, ElapsedEventArgs e)
    {
        int i = 0;
        foreach (Guid threadId in DataStreamProcessorV6.CurrentThreadState.Keys)
        {
            ThreadProgressState state = DataStreamProcessorV6.CurrentThreadState[threadId];
            decimal percent = state.LinesProcessedSoFar / (decimal)state.LinesToProcess;
            ConcurrentConsoleHelperDecorator.ColoredWriteLine($"[Thread {threadId}] {percent:P0}      (lines: {state.LinesProcessedSoFar:N0}/{state.LinesToProcess:N0}, " +
                                                                                                     $"bytes: {state.BytesReadSoFar:N0}/{state.BytesToRead:N0}, " +
                                                                                                     $"rows per sec: {state.LinesProcessedSoFar/state.Stopwatch.Elapsed.TotalSeconds:N0})", state.IsFinished ? ConsoleColor.Green : ConsoleColor.Yellow, 0, i++);
        }
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

        ConcurrentConsoleHelperDecorator.WriteLine($"\nExecution stats: Min: {timings.Min():N0}ms, Max: {timings.Max():N0}ms, Avg: {timings.Average():N0}ms");
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
        List<ResultRowV4> processedData = new();
        long executionTime = await TimeLogger.LogExecutionAsync($"Processing {rowCount:N0} rows using {processor.GetType().Name}", async () =>
        {
            processedData = await processor.ProcessData(filePath, rowCount, amountOfTasksInTotal);
        }, rowCount);
        IPresenterV4 presenter = new PresenterV4();
        string result = presenter.BuildResultString(processedData);
        
        if (Hasher.Hash(result) == correctHash)
        {
            ConcurrentConsoleHelperDecorator.WriteLine("Correct!");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            ConcurrentConsoleHelperDecorator.WriteLine("Incorrect!");
            ConcurrentConsoleHelperDecorator.WriteLine(Hasher.Hash(result));
            Console.ResetColor();
            
            string debug = File.ReadAllText($@"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-{rowCount.ToString("N0").Replace(".", "_")}.out").Trim();
            // ConsoleHelper.WriteLine($"You should have:\n{debug}");
            // ConsoleHelper.WriteLine($"You have:\n{result}");
        }

        return executionTime;
    }
}