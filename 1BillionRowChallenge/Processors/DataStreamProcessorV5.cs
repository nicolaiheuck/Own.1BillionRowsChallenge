using System.Collections.Concurrent;
using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V4:
///     Multi threading
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | 14 ms          |                                                   |
/// | 10,000        | 6 ms           |                                                   |
/// | 100,000       | 19 ms          |                                                   |
/// | 1,000,000     | 144 ms         |                                                   |
/// | 10,000,000    | 1374 ms        |                                                   |
/// | 1,000,000,000 | 126484 ms      |  7.906.084 (2.10 minutes or 1.67 using AOT)       |
/// Only 15 MB of memory
/// 7.9M rows a second (9.9M using AOT)
/// </summary>
//NH_TODO: For next versions
//             MemoryMappedFile.CreateFromFile
public class DataStreamProcessorV5 : IDataStreamProcessorV5
{
    private static int _linesProcessed;
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    private static SemaphoreSlim _semaphore;
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount, int? amountOfTasksInTotalOverwrite = null)
    {
        _linesProcessed = 0;
        _result = new();
        const int amountOfTasksInTotalConst = 10;
        const int amountOfTasksToRunInParallel = 10;
        _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        int amountOfTasksInTotal = amountOfTasksInTotalOverwrite ?? amountOfTasksInTotalConst;
        List<Task> tasks = [];

        int taskId = 0;
        for (int i = 0; i < amountOfTasksInTotal; i++)
        {
            int startAtLine = (int)(i * (rowCount / amountOfTasksInTotal));
            int stopAtLine = (int)((i + 1) * (rowCount / amountOfTasksInTotal));
            tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, startAtLine, stopAtLine), taskId++)));
        }

        await Task.WhenAll(tasks);
        return SecondLayerAggregation();
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

    private static void AggregateRows(IEnumerable<(string, int)> rows, int taskId)
    {
        _semaphore.Wait();

        try
        {
            // Console.WriteLine($"[{taskId}]: Starting");

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

                if (_linesProcessed % 1_000_000 == 0)
                {
                    Console.Write($"\rAggregated {_linesProcessed:N0} rows");
                }
            }
        }
        finally
        {
            // Console.WriteLine($"[{taskId}]: Stopping");
            _semaphore.Release();
        }
    }

    private static IEnumerable<ValueTuple<string, int>> ReadRowsFromFile(string filePath, int start, int end)
    {
        foreach (string line in File.ReadLines(filePath).Skip(start).Take(end - start))
        {
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture) * 100;
            yield return (cityNameSpan.ToString(), (int)temperature);
        }
    }
}