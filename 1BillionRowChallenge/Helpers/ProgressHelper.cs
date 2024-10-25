﻿using System.Diagnostics;
using System.Timers;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge.Helpers;

public class ProgressHelper
{
    public static bool Disabled { get; set; }
    
    private static System.Timers.Timer? _progressUpdater;
    private static readonly Stopwatch _startOfExecution = new();

    public static void StartProgressUpdater()
    {
        if (Disabled) return;
        Console.Clear();
        if (_progressUpdater != null) return;

        _progressUpdater = new(TimeSpan.FromMilliseconds(500));
        _progressUpdater.Start();
        _progressUpdater.Elapsed += UpdateThreadProgress;
    }
    
    public static void StopProgressUpdater()
    {
        if (Disabled) return;
        _progressUpdater?.Stop();
        // Console.Clear();
    }

    public static void StartExecutionTimer()
    {
        if (Disabled) return;
        _startOfExecution.Start();
    }

    public static void StopExecutionTimer()
    {
        if (Disabled) return;
        _startOfExecution.Stop();
    }

    private static void UpdateThreadProgress(object? sender, ElapsedEventArgs e)
    {
        if (Disabled) return;
        Console.Write($"\rProgress: {DataStreamProcessorV8.LinesProcessed:N0} ({DataStreamProcessorV8.LinesProcessed/_startOfExecution.Elapsed.TotalSeconds:N0} rows per sec, for {_startOfExecution.Elapsed.TotalSeconds:N0}s)");
        // int i = 0;
        // foreach ((var taskId, ThreadProgressState? state) in DataStreamProcessorV7.CurrentThreadState.ToList().OrderByDescending(s => s.Value.IsFinished).ThenByDescending(s => s.Value.LinesProcessedSoFar > 0))
        // {
        //     ConsoleColor consoleColor = GetColorForState(state);
        //     decimal percent = state.LinesProcessedSoFar / (decimal)state.LinesToProcess;
        //     // ConsoleHelper.ColoredWriteLine($"Threads: {DataStreamProcessorV7.CurrentThreadState.Count}", ConsoleColor.Green, 0, 0);
        //     ConsoleHelper.ColoredWriteLine($"[Thread {taskId}] {percent:P0}      (lines: {state.LinesProcessedSoFar:N0}/{state.LinesToProcess:N0}, " +
        //                                    $"bytes: {state.BytesReadSoFar:N0}/{state.BytesToRead:N0}, " +
        //                                    $"rows per sec: {state.LinesProcessedSoFar/state.Stopwatch.Elapsed.TotalSeconds:N0})                 ", consoleColor, 0, i++);
        // }
        //
        // var rowsPerSec = DataStreamProcessorV7.CurrentThreadState.Values.Sum(s => s.LinesProcessedSoFar) / _startOfExecution.Elapsed.TotalSeconds;
        // ConsoleHelper.ColoredWriteLine(new string(' ', 70), ConsoleColor.White, 0, i);
        // var rowsPerSecAverage = rowsPerSec / DataStreamProcessorV7.CurrentThreadState.Count;
        // ConsoleHelper.ColoredWriteLine($"Total rows per sec: {rowsPerSec:N0} ({rowsPerSecAverage:N0} avg) (has been running for {_startOfExecution.Elapsed.TotalSeconds:N0}s)", ConsoleColor.Green, 0, i + 1);
    }

    private static ConsoleColor GetColorForState(ThreadProgressState state)
    {
        if (state.IsFinished) return ConsoleColor.Green;
        if (state.LinesProcessedSoFar == 0) return ConsoleColor.DarkGray;
        return ConsoleColor.Yellow;
    }
}