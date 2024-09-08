using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V2:
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
public class DataStreamProcessorV3 : IDataStreamProcessor
{
    public List<ResultRow> ProcessData(string filePath)
    {
        IEnumerable<string> lines = ReadLinesFromFile(filePath);
        List<ResultRow> aggregatedDataPoints = AggregateDataPoints(lines);
        return aggregatedDataPoints;
    }

    private List<ResultRow> AggregateDataPoints(IEnumerable<string> lines)
    {
        int i = 0;
        Dictionary<string, AggregatedDataPoint> result = new();
        foreach (string line in lines)
        {
            DataPoint dataPoint = ParseLine(line); 
            AggregatedDataPoint aggregatedDataPoint;
            if (result.TryGetValue(dataPoint.CityName, out AggregatedDataPoint? value))
            {
                aggregatedDataPoint = value;
            }
            else
            {
                aggregatedDataPoint = new();
                result[dataPoint.CityName] = aggregatedDataPoint;
            }
            if (aggregatedDataPoint.Min == null || dataPoint.Temperature < aggregatedDataPoint.Min)
            {
                aggregatedDataPoint.Min = dataPoint.Temperature;
            }
            if (aggregatedDataPoint.Max == null || dataPoint.Temperature > aggregatedDataPoint.Max)
            {
                aggregatedDataPoint.Max = dataPoint.Temperature;
            }
            aggregatedDataPoint.Sum += dataPoint.Temperature;
            aggregatedDataPoint.AmountOfDataPoints++;

            i++;
            if (i % 100_000 == 0)
            {
                Console.Write($"\r Aggregated {i:N0} rows");
            }
        }

        return result.Select(keyPair => new ResultRow(keyPair.Key)
        {
            Min = keyPair.Value.Min,
            Max = keyPair.Value.Max,
            Mean = keyPair.Value.Sum / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private DataPoint ParseLine(string line)
    {
        ReadOnlySpan<char> lineAsSpan = line.AsSpan();
        int indexOfColon = lineAsSpan.IndexOf(';');
        
        ReadOnlySpan<char> cityName = lineAsSpan.Slice(0, indexOfColon);
        ReadOnlySpan<char> temperatureAsSpan = lineAsSpan.Slice(indexOfColon + 1);
        decimal temperature = decimal.Parse(temperatureAsSpan, CultureInfo.InvariantCulture);
        return new(cityName.ToString(), temperature);
    }

    private IEnumerable<string> ReadLinesFromFile(string filePath)
    {
        return File.ReadLines(filePath);
    }
}