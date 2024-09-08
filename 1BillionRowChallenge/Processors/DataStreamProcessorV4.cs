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
        Dictionary<string, AggregatedDataPoint> result = new();
        int i = 0;
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

    private IEnumerable<ValueTuple<string, decimal>> ReadRowsFromFile(string filePath)
    {
        using FileStream fileStream = File.OpenRead(filePath);

        int readByte = 0;
        byte[] cityNameBuffer = new byte[50];
        byte[] temperatureBuffer = new byte[50];
        int characterIndex = 0;
        int seperatorIndex = 0;
        int lineStartIndex = 0;
        do
        {
            readByte = fileStream.ReadByte();
            characterIndex++;

            if (readByte == ';')
            {
                seperatorIndex = (int)fileStream.Position;
                fileStream.Position = lineStartIndex;
                fileStream.Read(cityNameBuffer, 0, characterIndex - lineStartIndex - 1);
                fileStream.Position++;
            }
            else if (readByte == '\n')
            {
                fileStream.Position = seperatorIndex;
                fileStream.Read(temperatureBuffer, 0, characterIndex - seperatorIndex - 1);
                fileStream.Position++;
                string decimalAsString = System.Text.Encoding.UTF8.GetString(temperatureBuffer).Replace("\0", "");
                decimal temperature = decimal.Parse(decimalAsString, CultureInfo.InvariantCulture);
                string cityName = System.Text.Encoding.UTF8.GetString(cityNameBuffer).Replace("\0", "");
                yield return (cityName, temperature);
                lineStartIndex = (int)fileStream.Position;
                seperatorIndex = 0;
                cityNameBuffer = new byte[cityNameBuffer.Length];
                temperatureBuffer = new byte[temperatureBuffer.Length];
            }
        } while (readByte != -1);
    }
}