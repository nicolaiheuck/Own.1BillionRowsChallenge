using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V3:
///     
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second           |
/// |---------------|----------------|---------------------------|
/// | 10            | 9 ms           | 1,012                     |
/// | 10,000        | 9 ms           | 1,083,236                 |
/// | 100,000       | 35 ms          | 2,813,335                 |
/// | 1,000,000     | 365 ms         | 2,736,968                 |
/// | 10,000,000    | 2,470 ms       | 4,047,937                 |
/// | 1,000,000,000 | 243749ms       | 4.102.568 (4.0 minutes)   | //NH_TODO: Was incorrect :(
/// Only 20 MB of memory
/// 4.1M rows a second
/// </summary>
public class DataStreamProcessorV4 : IDataStreamProcessorV4 //NH_TODO: For V5. MemoryMappedFile.CreateFromFile
{
    public List<ResultRowV4> ProcessData(string filePath)
    {
        Dictionary<string, AggregatedDataPointV4> result = new();
        int i = 0;
        
        foreach ((string? cityName, decimal temperature) in ReadRowsFromFile(filePath))
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
            if (i % 1_000_000 == 0)
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

    private static IEnumerable<ValueTuple<string, decimal>> ReadRowsFromFile(string filePath)
    {
        foreach (string line in File.ReadLines(filePath))
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