﻿using System.Collections.Concurrent;
using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V4:
///     
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | 23 ms          |                                                   |
/// | 10,000        | 17 ms          |                                                   |
/// | 100,000       |                |                                                   |
/// | 1,000,000     |                |                                                   |
/// | 10,000,000    |                |                                                   |
/// | 1,000,000,000 | 157615 ms      |  6.544.561 (2.6 minutes or 2.1 using AOT)         |
/// Only ___ MB of memory
/// 6.3M rows a second (7.8M using AOT)
/// </summary>
//NH_TODO: For next versions
//             MultiThreading
//             MemoryMappedFile.CreateFromFile
public class DataStreamProcessorV5 : IDataStreamProcessorV5
{
    private static int _linesProcessed = 0;
    public static ConcurrentBag<string> ConcurrentLinesProcessed = new();
    private static ConcurrentDictionary<string, ConcurrentAggregatedDataPointV5> _result = new();
    private static object lockObject = new object();
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount)
    {
        _linesProcessed = 0;
        _result = new();
        ConcurrentLinesProcessed = new();
        FileInfo fileInfo = new(filePath);
        long rowsToProcess = rowCount;
        const int amountOfTasksToStart = 4;
        List<Task> tasks = [];

        for (int i = 0; i < amountOfTasksToStart; i++)
        {
            int startAtLine = (int)(i * (rowsToProcess / amountOfTasksToStart));
            int stopAtLine = (int)((i + 1) * (rowsToProcess / amountOfTasksToStart));
            tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, startAtLine, stopAtLine))));
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

    private static void AggregateRows(IEnumerable<(string, int)> rows)
    {
        foreach ((string? cityName, int temperature) in rows)
        {
            ConcurrentAggregatedDataPointV5 aggregatedDataPoint;
            if (_result.TryGetValue(cityName, out ConcurrentAggregatedDataPointV5? value))
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

            // Interlocked.Increment(ref _linesProcessed);
            // if (_linesProcessed % 1_000_000 == 0)
            // {
            //     Console.Write($"\rAggregated {_linesProcessed:N0} rows");
            // }
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
public class ConcurrentAggregatedDataPointV5
{
    public decimal Min;

    public decimal Max;

    public int Sum;

    public ulong AmountOfDataPoints;
}