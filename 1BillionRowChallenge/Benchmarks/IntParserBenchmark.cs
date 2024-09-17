using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge.Benchmarks;

[MemoryDiagnoser]
[BaselineColumn]
public class IntParserBenchmark
{
    private const string _line = "12";
    
    [Benchmark(Baseline = true)]
    [WarmupCount(2)]
    [IterationCount(5)]
    public int ParseIntUsingBuiltInIntParser()
    {
        return int.Parse(_line);
    }
    
    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public int ParseIntUsingMyParserV1()
    {
        return MyIntParserV1(_line);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int MyIntParserV1(ReadOnlySpan<char> line)
    {
        int result = 0;
        foreach (char letter in line)
        {
            result = result * 10 + (letter - '0');
        }

        return result;
    }

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ParserToTest(string line) => MyIntParserV1(line.AsSpan());
}