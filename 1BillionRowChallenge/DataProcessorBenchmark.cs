using System.Globalization;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Models;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge;

[MemoryDiagnoser]
[BaselineColumn]
public class DataProcessorBenchmark
{
    private const string _line = "Perth;25.5";
    
    // [Benchmark(Baseline = true)]
    // [WarmupCount(2)]
    // [IterationCount(5)]
    // public DataPoint ParseLineUsingStringSplitCalledTwice()
    // {
    //     string cityName = _line.Split(';')[0];
    //     string temperatureAsString = _line.Split(';')[1];
    //     decimal temperature = decimal.Parse(temperatureAsString, CultureInfo.InvariantCulture);
    //     return new(cityName, temperature);
    // }
    //
    // [Benchmark]
    // [WarmupCount(2)]
    // [IterationCount(5)]
    // public DataPoint ParseLineUsingIndexOfAndSubstring()
    // {
    //     int indexOfColon = _line.IndexOf(';');
    //     
    //     string cityName = _line.Substring(0, indexOfColon);
    //     string temperatureAsString = _line.Substring(indexOfColon + 1);
    //     decimal temperature = decimal.Parse(temperatureAsString, CultureInfo.InvariantCulture);
    //     return new(cityName, temperature);
    // }
    
    // [Benchmark]
    // [WarmupCount(2)]
    // [IterationCount(5)]
    // public DataPoint ParseLineUsingStringSplitCalledOnce()
    // {
    //     string[] splitLine = _line.Split(';');
    //     string cityName = splitLine[0];
    //     string temperatureAsString = splitLine[1];
    //     decimal temperature = decimal.Parse(temperatureAsString, CultureInfo.InvariantCulture);
    //     return new(cityName, temperature);
    // }
    
    [Benchmark(Baseline = true)]
    [WarmupCount(2)]
    [IterationCount(5)]
    public DataPoint ParseLineUsingSpans()
    {
        ReadOnlySpan<char> lineAsSpan = _line.AsSpan();
        int indexOfColon = lineAsSpan.IndexOf(';');
        
        ReadOnlySpan<char> cityName = lineAsSpan.Slice(0, indexOfColon);
        ReadOnlySpan<char> temperatureAsSpan = lineAsSpan.Slice(indexOfColon + 1);
        decimal temperature = decimal.Parse(temperatureAsSpan, CultureInfo.InvariantCulture);
        return new(cityName.ToString(), temperature);
    }
    
    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public DataPoint ParseLineUsingOwnIndexOf()
    {
        ReadOnlySpan<char> lineAsSpan = _line.AsSpan();
        ReadOnlySpan<char> cityName = null;
        ReadOnlySpan<char> temperatureAsSpan = null;
        for (int i = 0; i < lineAsSpan.Length; i++)
        {
            if (lineAsSpan[i] == ';')
            {
                cityName = lineAsSpan.Slice(0, i);
                temperatureAsSpan = lineAsSpan.Slice(i + 1);
                break;
            }
        }
        
        decimal temperature = decimal.Parse(temperatureAsSpan, CultureInfo.InvariantCulture);
        return new(cityName.ToString(), temperature);
    }
}