using System.Collections.Concurrent;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V5:
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | __ ms          |                                                   |
/// | 10,000        | _ ms           |                                                   |
/// | 100,000       | __ ms          |                                                   |
/// | 1,000,000     | ___ ms         |                                                   |
/// | 10,000,000    | ____ ms        |                                                   |
/// | 1,000,000,000 | ______ ms      |  _________ (____ minutes or ____ using AOT)       |
/// Only __ MB of memory
/// ___M rows a second (___M using AOT)
/// </summary>
//NH_TODO: For next versions
//             Split processing into a first and second pass
//             MemoryMappedFile.CreateFromFile
public class DataStreamProcessorV6 : IDataStreamProcessorV5
{
    private static int _linesProcessed;
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    public static ConcurrentDictionary<Guid, ThreadProgressState> CurrentThreadState = new();
    private static SemaphoreSlim _semaphore = null!;
    private static List<Block> _blocks = [];
    
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount, int? amountOfTasksInTotalOverwrite = null)
    {
        const int amountOfTasksToRunInParallel = 1;
        _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        _linesProcessed = 0;
        _result = new();
        CurrentThreadState = new();

        TaskFactory taskFactory = new(TaskCreationOptions.RunContinuationsAsynchronously, TaskContinuationOptions.ExecuteSynchronously);
        _blocks = FileSplitter.SplitFileIntoBlocks(filePath, 10);
        using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(filePath);
        List<Task> tasks = [];
        foreach (Block block in _blocks)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    CurrentThreadState[block.Id] = new() { LinesToProcess = rowCount / _blocks.Count, BytesToRead = block.End - block.Start };
                    using MemoryMappedViewStream viewStream = memoryMappedFile.CreateViewStream(block.Start, block.End - block.Start, MemoryMappedFileAccess.Read);
                    IEnumerable<(string, int)> rows = ReadRowsFromFile(viewStream , block);
                    await AggregateRows(rows, block.Id);
                }
                catch (Exception ex)
                {
                    ConsoleHelper.WriteAtPosition(0, 10, $"Error while running task {block.Id}:");
                    ConsoleHelper.WriteAtPosition(0, 11, ex.Message);
                    throw;
                }
            }));
            // BoundaryTester.BoundaryTest(filePath, block);
            
        }
        await Task.WhenAll(tasks);
        Console.Clear();
        ConsoleHelper.WriteAtPosition(0, 10, "First pass done. Press a key to continue");
        Console.ReadKey();

        // Block lastBlock = blocks.Last();
        //
        // Console.Clear();
        // ConsoleHelper.WriteLine("Press a key to start the second pass");
        // Console.ReadKey(true);
        // blocks = SplitFileIntoBlocks(filePath, 10, lastBlock.Start);
        // foreach (Block block in blocks)
        // {
        //     long rowCount = block.End - block.Start;
        //     tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, block.Start, block.End), taskId++, 10_000_000)));
        // }
        // await Task.WhenAll(tasks);
        //
        return SecondLayerAggregation();
    }

    private static async Task AggregateRows(IEnumerable<(string, int)> rows, Guid taskId)
    {
        await _semaphore.WaitAsync();

        long i = 0;
        
        try
        {
            foreach ((string? cityName, int temperature) in rows)
            {
                AggregatedDataPointV5 aggregatedDataPoint;

                if (_result.TryGetValue(cityName, out AggregatedDataPointV5? value))
                {
                    aggregatedDataPoint = value;
                }
                else
                {
                    aggregatedDataPoint = new()
                    {
                        Min = int.MaxValue,
                        Max = int.MinValue,
                    };

                    _result[cityName] = aggregatedDataPoint;
                }

                if (temperature < aggregatedDataPoint.Min)
                {
                    aggregatedDataPoint.Min = temperature;
                }

                if (temperature > aggregatedDataPoint.Max)
                {
                    aggregatedDataPoint.Max = temperature;
                }

                Interlocked.Add(ref aggregatedDataPoint.Sum, temperature);
                Interlocked.Increment(ref aggregatedDataPoint.AmountOfDataPoints);

                Interlocked.Increment(ref _linesProcessed);
                i++;

                CurrentThreadState[taskId].LinesProcessedSoFar++;
                // if (i % 10_000_000 == 0)
                // {
                //     return;
                //     //     // ConsoleHelper.Write($"\rAggregated {_linesProcessed:N0} rows");
                // }
            }
        }
        finally
        {
            _semaphore.Release();
            CurrentThreadState[taskId].IsFinished = true;
            CurrentThreadState[taskId].Stopwatch.Stop();
        }
    }

    private static List<ResultRowV4> SecondLayerAggregation()
    {
        return _result.Select(keyPair => new ResultRowV4(keyPair.Key)
        {
            Min = keyPair.Value.Min / 100,
            Max = keyPair.Value.Max / 100,
            Mean = ((decimal)keyPair.Value.Sum / 100) / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private static IEnumerable<(string, int)> ReadRowsFromFile(MemoryMappedViewStream viewStream, Block block)
    {
        CurrentThreadState[block.Id].Stopwatch.Start();
        using StreamReader reader = new(viewStream);
        
        string? line = reader.ReadLine();
        do
        {
            CurrentThreadState[block.Id].BytesReadSoFar += line?.Length ?? 0;

            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture) * 100;
            yield return (cityNameSpan.ToString(), (int)temperature);
            line = reader.ReadLine();
        } while (line != null);
    }
}