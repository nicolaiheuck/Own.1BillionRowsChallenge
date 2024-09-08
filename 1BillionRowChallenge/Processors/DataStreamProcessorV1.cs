using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Benchmarks:
///     10:         8       ms
///     10.000:     18      ms
///     100.000:    80      ms
///     1.000.000:  1.069   ms
///     10.000.000: 9.652   ms
///     1.000.000.000: Killed after using 20 GB of memory
/// </summary>
public class DataStreamProcessorV1 : IDataStreamProcessor
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
        List<ResultRow> result = new();
        List<IGrouping<string, DataPoint>> group = dataPoints.GroupBy(d => d.CityName).ToList();

        foreach (IGrouping<string, DataPoint> dataPoint in group)
        {
            ResultRow row = new(dataPoint.Key)
            {
                Min = dataPoint.Min(d => d.Temperature),
                Max = dataPoint.Max(d => d.Temperature),
                Mean = dataPoint.Average(d => d.Temperature),
            };

            result.Add(row);
        }
        
        return result;
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