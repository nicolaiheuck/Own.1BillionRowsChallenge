using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V3:
///     
/// 
/// Benchmarks:
///    | File Size        | Execution Time           |
///    |------------------|--------------------------|
///    | 10               |                      |
///    | 10,000           |                      |
///    | 100,000          |                     |
///    | 1,000,000        | 
///    | 10,000,000       |                  |
///    | 1,000,000,000    |  |
/// Only _________________ MB of memory
/// ___ rows a second
/// </summary>
public class DataStreamProcessorV4 : IDataStreamProcessorV4
{
    public List<ResultRowV4> ProcessData(string filePath)
    {
        Dictionary<string, AggregatedDataPointV4> result = new();
        int i = 0;
        
        foreach ((string? cityName, double temperature) in ReadRowsFromFile(filePath))
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
            
            i++;
            if (i % 100_000 == 0)
            {
                Console.Write($"\rAggregated {i:N0} rows");
            }
        }
        
        return result.Select(keyPair => new ResultRowV4(keyPair.Key)
        {
            Min = keyPair.Value.Min,
            Max = keyPair.Value.Max,
            Mean = keyPair.Value.Sum / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private static IEnumerable<ValueTuple<string, double>> ReadRowsFromFile(string filePath)
    {
        foreach (string line in File.ReadLines(filePath))
        {
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            double temperature = double.Parse(temperatureSpan, CultureInfo.InvariantCulture);
            yield return (cityNameSpan.ToString(), temperature);
        }
    }
}