using System.Globalization;
using System.IO.MemoryMappedFiles;
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
/// | 10            |                |                                                   |
/// | 10,000        |                |                                                   |
/// | 100,000       |                |                                                   |
/// | 1,000,000     |                |                                                   |
/// | 10,000,000    |                |                                                   |
/// | 1,000,000,000 |                |                                                   |
/// Only ___ MB of memory
/// ____ rows a second (___ using AOT)
/// </summary>
//NH_TODO: For next versions
//             MultiThreading
//             MemoryMappedFile.CreateFromFile
public class DataStreamProcessorV5 : IDataStreamProcessorV5
{
    private static int _linesProcessed = 0;
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount)
    {
        FileInfo fileInfo = new(filePath);
        long rowsToProcess = rowCount;
        const int amountOfTasksToStart = 2;
        List<Task<Dictionary<string, AggregatedDataPointV4>>> tasks = [];

        for (int i = 0; i < amountOfTasksToStart; i++)
        {
            long startAtLine = i * (rowsToProcess / amountOfTasksToStart);
            long stopAtLine = (i + 1) * (rowsToProcess / amountOfTasksToStart);
            Console.WriteLine($"Reading: {startAtLine} to {stopAtLine}");
            tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, (int)startAtLine, (int)stopAtLine))));
        }

        Dictionary<string, AggregatedDataPointV4>[] results = await Task.WhenAll(tasks);
        return SecondLayerAggregation(results.ToList());
    }
    
    private static List<ResultRowV4> SecondLayerAggregation(List<Dictionary<string, AggregatedDataPointV4>> results)
    {
        Dictionary<string, AggregatedDataPointV4> aggregation = [];
        foreach (Dictionary<string, AggregatedDataPointV4> result in results)
        {
            foreach ((string? cityName, AggregatedDataPointV4? dataPoint) in result)
            {
                if (aggregation.ContainsKey(cityName))
                {
                    AggregatedDataPointV4? existingDataPoint = aggregation[cityName];
                    existingDataPoint.Min = Math.Min(existingDataPoint.Min, dataPoint.Min);
                    existingDataPoint.Max = Math.Max(existingDataPoint.Max, dataPoint.Max);
                    existingDataPoint.Sum += dataPoint.Sum;
                    existingDataPoint.AmountOfDataPoints += dataPoint.AmountOfDataPoints;
                }
                else
                {
                    aggregation[cityName] = dataPoint;
                }
            }
        }

        return aggregation.Select(keyPair => new ResultRowV4(keyPair.Key)
        {
            Min = keyPair.Value.Min,
            Max = keyPair.Value.Max,
            Mean = keyPair.Value.Sum / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private static Dictionary<string, AggregatedDataPointV4> AggregateRows(IEnumerable<(string, decimal)> rows)
    {
        Dictionary<string, AggregatedDataPointV4> result = new();
        
        foreach ((string? cityName, decimal temperature) in rows)
        {
            AggregatedDataPointV4 aggregatedDataPoint;
            if (result.TryGetValue(cityName, out AggregatedDataPointV4? value))
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
                result[cityName] = aggregatedDataPoint;
                
            }
            if (temperature < aggregatedDataPoint.Min)
            {
                aggregatedDataPoint.Min = temperature;
            }
            if (temperature > aggregatedDataPoint.Max)
            {
                aggregatedDataPoint.Max = temperature;
            }
            aggregatedDataPoint.Sum += temperature;
            aggregatedDataPoint.AmountOfDataPoints++;

            Interlocked.Increment(ref _linesProcessed);
            if (_linesProcessed % 1_000_000 == 0)
            {
                Console.Write($"\rAggregated {_linesProcessed:N0} rows");
            }
        }

        return result;
    }

    private static IEnumerable<ValueTuple<string, decimal>> ReadRowsFromFile(string filePath, int skip, int take)
    {
        foreach (string line in File.ReadLines(filePath).Skip(skip).Take(take))
        {
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture);
            yield return (cityNameSpan.ToString(), temperature);
        }
    }
}