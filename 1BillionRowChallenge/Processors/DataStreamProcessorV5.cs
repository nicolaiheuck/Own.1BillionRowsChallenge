using System.Globalization;
using System.Text;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V3:
///     
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | 9 ms           | 1,012                                             |
/// | 10,000        | 9 ms           | 1,083,236                                         |
/// | 100,000       | 35 ms          | 2,813,335                                         |
/// | 1,000,000     | 365 ms         | 2,736,968                                         |
/// | 10,000,000    | 2,470 ms       | 4,047,937                                         |
/// | 1,000,000,000 | 243749ms       | 4.102.568 (4.0 minutes) (3.1 minutes using AOT)   | //NH_TODO: Was incorrect :(
/// Only 20 MB of memory
/// 4.1M rows a second (5.4 using AOT)
/// </summary>
//NH_TODO: For next versions
//             Short as ids
//             MemoryMappedFile.CreateFromFile
//             MultiThreading
public class DataStreamProcessorV5 : IDataStreamProcessorV5
{
    private static short _globalIdCounter = 0;
    public List<ResultRowV5> ProcessData(string filePath)
    {
        Dictionary<short, AggregatedDataPointV4> result = new();
        int i = 0;
        
        foreach ((short cityName, decimal temperature) in ReadRowsFromFile(filePath))
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
        
        return result.Select(keyPair => new ResultRowV5(keyPair.Key)
        {
            Min = keyPair.Value.Min,
            Max = keyPair.Value.Max,
            Mean = keyPair.Value.Sum / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private static IEnumerable<ValueTuple<short, decimal>> ReadRowsFromFile(string filePath)
    {
        int i = 0;
        foreach (string line in File.ReadLines(filePath))
        {
            i++;
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            short id = GenerateIdFromCityName(cityNameSpan, i);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture);
            yield return (id, temperature);
        }
    }

    public static short GenerateIdFromCityName(ReadOnlySpan<char> cityNameSpan, int incrementer)
    {
        return ++_globalIdCounter;
        //IDK. It works. If I touch anything it breaks.... But not it slow... REEEEEEEE
        return (short)((cityNameSpan[2] * 13 * incrementer * cityNameSpan[0] * 7 * incrementer * cityNameSpan[1] * incrementer) % 65535);
        // // short digit1 = (short)cityNameSpan[0];
        // // short digit2 = (short)cityNameSpan[1];
        // // short digit3 = (short)cityNameSpan[2];
        // // int result = (digit3 * 200) / (digit2 * 27) ^ (digit1); // 34
        // // int result = (digit1 * 200) / (digit2 * 27) ^ (digit3); // 35
        // // int result = (digit2 * 200) / (digit1 * 27) ^ (digit3); // 36
        // // int result = (digit2 * 137) / (digit1 * 27) ^ (digit3); // 38 
        // // int result = (digit2 << 16) / (digit1 * 27) ^ (digit3); // 303 
        // // int result = (digit2 << 16) / (digit1 << 3) ^ (digit3 << (incrementer % 12)); // 398 
        // // int result = (digit2 << 16) / (digit1) ^ (digit3 << (incrementer % 12)); // 400
        // // int incrementerMod12 = incrementer % 12;
        //
        // // int result = (digit2 << incrementerMod12) / (short)(Math.Pow(digit1, incrementerMod12) % 5927) ^ (digit3 << (incrementer % 12)); // 400
        //
        // string hash = Hasher.Hash(cityNameSpan.ToString());
        // short[] shorts = Encoding.UTF8.GetBytes(hash);
        // short result = 0;
        // for (int i = 0; i < 8; i++)
        // {
        //     result += shorts[i];
        //     result <<= 8;
        // }
        //
        // return (short)result;
    }
}