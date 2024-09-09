using System.Text;
using _1BillionRowChallenge.Helpers;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge.Benchmarks;

[MemoryDiagnoser]
[BaselineColumn]
public class IdGeneratorBenchmark
{
    private List<string> _cityNames = null!;

    [GlobalSetup]
    public void Setup()
    {
        _cityNames = File.ReadAllLines(FilePathConstants.Measurements10_000)
                         .Select(l => l.Split(";")[0])
                         .DistinctBy(l => l)
                         .ToList();
    }

    [Benchmark(Baseline = true)]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<ulong> GenerateIdUsingHasher()
    {
        List<ulong> results = new();
        foreach (string cityName in _cityNames)
        {
            string hash = Hasher.Hash(cityName);
            byte[] bytes = Encoding.UTF8.GetBytes(hash);
            ulong result = 0;
            for (int i = 0; i < 8; i++)
            {
                result += bytes[i];
                result <<= 8;
            }

            results.Add(result);
        }
        
        return results;
    }

    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public List<ulong> GenerateIdUsingCustomAlgorithm()
    {
        List<ulong> results = new();
        int i = 0;
        foreach (string cityName in _cityNames)
        {
            i++;
            ReadOnlySpan<char> cityNameSpan = cityName.AsSpan();
            ulong id = (ulong)((cityNameSpan[2] * 13 * i * cityNameSpan[0] * 7 * i * cityNameSpan[1] * i) % 65535);
            results.Add(id);
        }
        
        return results;
    }
}