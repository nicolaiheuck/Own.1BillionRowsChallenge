using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V1:
///     Instead of using group by it uses a dictionary and therefore shouldn't need to create a list of 1B objects
/// 
/// Benchmarks:
///    | File Size        | Execution Time           |
///    |------------------|--------------------------|
///    | 10               | 7 ms                     |
///    | 10,000           | 8 ms                     |
///    | 100,000          | 46 ms                    |
///    | 1,000,000        | 542 ms                   |
///    | 10,000,000       | 3,931 ms                 |
///    | 1,000,000,000    | 389,512 ms (6.4 minutes) |
/// Only 18,61 MB of memory
/// </summary>
public class DataStreamProcessorV2 : IDataStreamProcessor
{
    public List<ResultRow> ProcessData(string filePath)
    {
        IEnumerable<string> data = ReadLinesFromFile(filePath);
        IEnumerable<DataPoint> dataPoints = ParseDataFromFile(data);
        List<ResultRow> aggregatedDataPoints = AggregateDataPoints(dataPoints);
        return aggregatedDataPoints;
    }

    private List<ResultRow> AggregateDataPoints(IEnumerable<DataPoint> dataPoints)
    {
        int i = 0;
        Dictionary<string, AggregatedDataPoint> result = new();
        foreach (DataPoint dataPoint in dataPoints)
        {
            AggregatedDataPoint aggregatedDataPoint = null;
            if (result.ContainsKey(dataPoint.CityName))
            {
                aggregatedDataPoint = result[dataPoint.CityName];
            }
            else
            {
                aggregatedDataPoint = new();
                result[dataPoint.CityName] = aggregatedDataPoint;
            }
            if (dataPoint.Temperature < aggregatedDataPoint.Min || aggregatedDataPoint.Min == null) // NH_TODO: Does it need to check for null? Would it not return false anyway?
            {
                aggregatedDataPoint.Min = dataPoint.Temperature;
            }
            if (dataPoint.Temperature > aggregatedDataPoint.Max || aggregatedDataPoint.Max == null)
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

    private IEnumerable<DataPoint> ParseDataFromFile(IEnumerable<string> lines)
    {
        foreach (string line in lines)
        {
            string[] splitValue = line.Split(";");
            string cityName = splitValue[0];
            string temperatureAsString = splitValue[1];
            decimal temperature = decimal.Parse(temperatureAsString, CultureInfo.InvariantCulture);
            yield return new(cityName, temperature);
        }
    }

    private IEnumerable<string> ReadLinesFromFile(string filePath)
    {
        return File.ReadLines(filePath);
    }
}