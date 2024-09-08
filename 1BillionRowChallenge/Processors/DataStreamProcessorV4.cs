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
public class DataStreamProcessorV4 : IDataStreamProcessor
{
    public List<ResultRow> ProcessData(string filePath)
    {
        using FileStream fileStream = File.OpenRead(filePath);
        using StreamReader reader = new(fileStream);
        Dictionary<string, AggregatedDataPoint> result = new();
        int c = 0;
        
        foreach ((string? cityName, decimal temperature) in ReadRowsFromFile(filePath))
        {
            AggregatedDataPoint aggregatedDataPoint;
            if (result.TryGetValue(cityName, out AggregatedDataPoint? value))
            {
                aggregatedDataPoint = value;
            }
            else
            {
                aggregatedDataPoint = new();
                result[cityName] = aggregatedDataPoint;
            }
            if (aggregatedDataPoint.Min == null || temperature < aggregatedDataPoint.Min)
            {
                aggregatedDataPoint.Min = temperature;
            }
            if (aggregatedDataPoint.Max == null || temperature > aggregatedDataPoint.Max)
            {
                aggregatedDataPoint.Max = temperature;
            }
            aggregatedDataPoint.Sum += temperature;
            aggregatedDataPoint.AmountOfDataPoints++;
            
            c++;
            if (c % 100_000 == 0)
            {
                Console.Write($"\rAggregated {c:N0} rows");
            }
        }
        
        return result.Select(keyPair => new ResultRow(keyPair.Key)
        {
            Min = keyPair.Value.Min,
            Max = keyPair.Value.Max,
            Mean = keyPair.Value.Sum / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    // private List<ResultRow> AggregateDataPoints(string cityName, decimal temperature)
    // {
    //
    // }

    // private DataPoint ParseLine(string line)
    // {
    //     ReadOnlySpan<char> lineAsSpan = line.AsSpan();
    //     int indexOfColon = lineAsSpan.IndexOf(';');
    //     
    //     ReadOnlySpan<char> cityName = lineAsSpan.Slice(0, indexOfColon);
    //     ReadOnlySpan<char> temperatureAsSpan = lineAsSpan.Slice(indexOfColon + 1);
    //     decimal temperature = decimal.Parse(temperatureAsSpan, CultureInfo.InvariantCulture);
    //     return new(cityName.ToString(), temperature);
    // }

    private static IEnumerable<ValueTuple<string, decimal>> ReadRowsFromFile(string filePath)
    {
        using FileStream fileStream = File.OpenRead(filePath);
        using StreamReader reader = new(fileStream);
        
        do
        {
            string? line = reader.ReadLine();
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture);
            yield return (cityNameSpan.ToString(), temperature);
        } while (!reader.EndOfStream);
    }
}