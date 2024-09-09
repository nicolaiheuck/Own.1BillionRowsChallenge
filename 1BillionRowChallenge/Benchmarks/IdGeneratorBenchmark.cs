using System.Text;
using _1BillionRowChallenge.Helpers;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge.Benchmarks;

[MemoryDiagnoser]
[BaselineColumn]
public class IdGeneratorBenchmark
{
    private List<string> _cityNames = null!;
    private static short _globalIdCounter = 0;

    [GlobalSetup]
    public void Setup()
    {
        _cityNames = File.ReadAllLines(FilePathConstants.Measurements10_000)
                         .Select(l => l.Split(";")[0])
                         .DistinctBy(l => l)
                         .ToList();
    }

    // [Benchmark]
    // [WarmupCount(2)]
    // [IterationCount(5)]
    // public List<ulong> GenerateIdUsingHasher()
    // {
    //     List<ulong> results = new();
    //     foreach (string cityName in _cityNames)
    //     {
    //         string hash = Hasher.Hash(cityName);
    //         byte[] bytes = Encoding.UTF8.GetBytes(hash);
    //         ulong result = 0;
    //         for (int i = 0; i < 8; i++)
    //         {
    //             result += bytes[i];
    //             result <<= 8;
    //         }
    //
    //         results.Add(result);
    //     }
    //     
    //     return results;
    // }

    [Benchmark(Baseline = true)]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<short> GenerateIdUsingCustomAlgorithm()
    {
        List<short> results = new();
        int i = 0;
        foreach (string cityName in _cityNames)
        {
            i++;
            ReadOnlySpan<char> cityNameSpan = cityName.AsSpan();
            short id = (short)((cityNameSpan[2] * 13 * i * cityNameSpan[0] * 7 * i * cityNameSpan[1] * i) % 65535);
            results.Add(id);
        }
        
        return results;
    }

    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<short> GenerateIdUsingSecondSimplerAlgorithm()
    {
        List<short> results = new();
        int i = 0;

        foreach (string cityName in _cityNames)
        {
            ReadOnlySpan<char> cityNameSpan = cityName.AsSpan();
            short digit1 = (short)cityNameSpan[0];
            short digit2 = (short)cityNameSpan[1];
            short digit3 = (short)cityNameSpan[2];
            int result = (digit2 << 16) / (digit1) ^ (digit3 << (i % 12)); // 400

            results.Add((short)result);
        }

        return results;
    }

    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<short> GenerateIdUsingThirdSimplerAlgorithm()
    {
        List<short> results = new();
        int i = 0;

        foreach (string cityName in _cityNames)
        {
            ReadOnlySpan<char> cityNameSpan = cityName.AsSpan();
            
            short digit1 = (short)cityNameSpan[0];
            short digit2 = (short)cityNameSpan[1];
            short digit3 = (short)cityNameSpan[2];
            
            int incrementerMod12 = i % 12;
            int result = (digit2 << 4) / (digit1) ^ (digit3 << incrementerMod12) * (i + digit2);
        
            results.Add((short)(result % short.MaxValue));
        }

        return results;
    }

    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<ushort> GenerateId_WithoutIncrement_UsingFirstAlgorithm()
    {
        List<ushort> results = new();
        int i = 0;

        foreach (string cityName in _cityNames)
        {
            ReadOnlySpan<char> cityNameSpan = cityName.AsSpan();
            ulong result = 3;
            foreach (char letter in cityNameSpan)
            {
                result *= letter;
                result -= letter + 3u;
            }
            results.Add((ushort)(result % 65535));
        }

        return results;
    }
}