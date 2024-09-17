using System.Globalization;
using System.Runtime.CompilerServices;
using _1BillionRowChallenge.Models;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge.Benchmarks;

[MemoryDiagnoser]
[BaselineColumn]
public class DecimalParsingBenchmark
{
    private const string _line = "25.5";
    
    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public decimal ParseDecimalUsingBuiltInDecimalParser()
    {
        return decimal.Parse(_line, CultureInfo.InvariantCulture);
    }
    
    [Benchmark(Baseline = true)]
    [WarmupCount(2)]
    [IterationCount(5)]
    public decimal ParseDecimalUsingOwnDecimalParser()
    {
        return MyDecimalParserV9(_line);
    }
    
    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public decimal ParseDecimalUsingOwnDecimalParserV10()
    {
        return MyDecimalParserV10(_line);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static decimal ParserToTest(string line) => MyDecimalParserV10(line);

    /// <summary>
    /// 
    /// </summary>
    private unsafe static decimal MyDecimalParserV10(string line) // 
    {
        fixed (char* linePointer = line)
        {
            char* readingByte = linePointer;
            bool isNegative = *readingByte == '-';
        
            int isNegativityMultiplier = 1;
            if (isNegative)
            {
                isNegativityMultiplier = -1;
                readingByte++;
            }
            int result = 0;
            while (*readingByte != '.')
            {
                result = result * 10 + (*readingByte - '0');
                readingByte++;
            }
        
            result *= 10;
            readingByte++;
            int afterDot = *readingByte - '0';
        
            decimal debug = (result + afterDot) * isNegativityMultiplier;
            return debug;
        }
    }

    /// <summary>
    /// Saved 0.8 ns by using pointers (with fixed and unsafe)
    /// </summary>
    private unsafe static decimal MyDecimalParserV9(string line) // 1.803 ns //NH_TODO: Implement
    {
        fixed (char* linePointer = line)
        {
            char* readingByte = linePointer;
            bool isNegative = *readingByte == '-';
        
            int isNegativityMultiplier = 1;
            if (isNegative)
            {
                isNegativityMultiplier = -1;
                readingByte++;
            }
            int result = 0;
            while (*readingByte != '.')
            {
                result = result * 10 + (*readingByte - '0');
                readingByte++;
            }
        
            result *= 10;
            readingByte++;
            int afterDot = *readingByte - '0';
        
            decimal debug = (result + afterDot) * isNegativityMultiplier;
            return debug;
        }
    }

    /// <summary>
    /// Saved 38 ns by not dividing by 10 and multiplying by 10 instead
    /// </summary>
    private static decimal MyDecimalParserV8(ReadOnlySpan<char> line) // 2.603 ns
    {
        ref char firstByte = ref Unsafe.AsRef(in line[0]);
        bool isNegative = firstByte == '-';

        int negativityMultiplier = 1;
        if (isNegative)
        {
            negativityMultiplier = -1;
            firstByte = ref Unsafe.Add(ref firstByte, 1);
        }
        int result = 0;
        while (firstByte != '.')
        {
            result = result * 10 + (firstByte - '0');
            firstByte = ref Unsafe.Add(ref firstByte, 1);
        }

        result *= 10;
        ref char afterDotByte = ref Unsafe.Add(ref firstByte, 1);
        int afterDot = afterDotByte - '0';

        decimal debug = (result + afterDot) * negativityMultiplier;
        return debug;
    }
    

    private static decimal MyDecimalParserV1(string line) // 69.192 ns
    {
        bool isNegative = line.StartsWith('-');
        int startIndex = isNegative ? 1 : 0;

        int indexOfDot = line.IndexOf('.');
        int firstPart = int.Parse(line[startIndex..indexOfDot]);
        int secondPart = int.Parse(line[(indexOfDot + 1)..]);
        decimal secondPartAsDecimal = secondPart / 10m;
        return (firstPart + secondPartAsDecimal) * (isNegative ? -1 : 1);
    }

    /// <summary>
    /// Saved 10 ns by using ReadOnlySpan<char> instead of string
    /// </summary>
    private static decimal MyDecimalParserV2(ReadOnlySpan<char> line) // 59.666 ns
    {
        bool isNegative = line.StartsWith("-");
        int startIndex = isNegative ? 1 : 0;

        int indexOfDot = line.IndexOf('.');
        int firstPart = int.Parse(line[startIndex..indexOfDot]);
        int secondPart = int.Parse(line[(indexOfDot + 1)..]);
        decimal secondPartAsDecimal = secondPart / 10m;
        return (firstPart + secondPartAsDecimal) * (isNegative ? -1 : 1);
    }

    /// <summary>
    /// Saved 1 ns by using Slice instead of [..]
    /// </summary>
    private static decimal MyDecimalParserV3(ReadOnlySpan<char> line) // 58.633 ns
    {
        bool isNegative = line.StartsWith("-");
        int negativityMultiplier = isNegative ? -1 : 1;
        int startIndex = isNegative ? 1 : 0;

        int indexOfDot = line.IndexOf('.');
        int firstPart = int.Parse(line.Slice(startIndex, indexOfDot - startIndex));
        int secondPart = int.Parse(line.Slice(indexOfDot + 1));
        decimal secondPartAsDecimal = secondPart / 10m;
        return (firstPart + secondPartAsDecimal) * negativityMultiplier;
    }

    /// <summary>
    /// Saved 1 ns by using line[0] instead of line.StartsWith("-")
    /// </summary>
    private static decimal MyDecimalParserV4(ReadOnlySpan<char> line) // 57.654 ns 
    {
        bool isNegative = line[0] == '-';
        int negativityMultiplier = isNegative ? -1 : 1;
        int startIndex = isNegative ? 1 : 0;

        int indexOfDot = line.IndexOf('.');
        int firstPart = int.Parse(line.Slice(startIndex, indexOfDot - startIndex));
        int secondPart = int.Parse(line.Slice(indexOfDot + 1));
        decimal secondPartAsDecimal = secondPart / 10m;
        return (firstPart + secondPartAsDecimal) * negativityMultiplier;
    }

    /// <summary>
    /// Saved 13 ns by using MyIntParserV1 instead of int.Parse
    /// </summary>
    private static decimal MyDecimalParserV5(ReadOnlySpan<char> line) // 44.245 ns
    {
        bool isNegative = line[0] == '-';
        int negativityMultiplier = isNegative ? -1 : 1;
        int startIndex = isNegative ? 1 : 0;

        int indexOfDot = line.IndexOf('.');
        int firstPart = MyIntParserV1(line.Slice(startIndex, indexOfDot - startIndex));
        int secondPart = MyIntParserV1(line.Slice(indexOfDot + 1));
        decimal secondPartAsDecimal = secondPart / 10m;
        return (firstPart + secondPartAsDecimal) * negativityMultiplier;
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

    /// <summary>
    /// Saved 8 ns by ignoring the dot and dividing by 10 using custom int parser
    /// </summary>
    private static decimal MyDecimalParserV6(ReadOnlySpan<char> line) // 36.065 ns
    {
        bool isNegative = line[0] == '-';
        int negativityMultiplier = isNegative ? -1 : 1;
        int startIndex = isNegative ? 1 : 0;

        int firstPart = MyIntParserV2(line.Slice(startIndex));
        return (firstPart / 10m) * negativityMultiplier;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int MyIntParserV2(ReadOnlySpan<char> line)
    {
        int result = 0;
        foreach (char letter in line)
        {
            if (letter == '.') continue;
            result = result * 10 + (letter - '0');
        }

        return result;
    }

    /// <summary>
    /// Experimented with ref and Unsafe.Add to see if it would be faster
    /// </summary>
    private static decimal MyDecimalParserV7(ReadOnlySpan<char> line) // 40.325 ns
    {
        ref char firstByte = ref Unsafe.AsRef(in line[0]);
        bool isNegative = firstByte == '-';

        int negativityMultiplier = 1;
        if (isNegative)
        {
            negativityMultiplier = -1;
            firstByte = ref Unsafe.Add(ref firstByte, 1);
        }
        int result = 0;
        while (firstByte != '.')
        {
            result = result * 10 + (firstByte - '0');
            firstByte = ref Unsafe.Add(ref firstByte, 1);
        }
        ref char afterDotByte = ref Unsafe.Add(ref firstByte, 1);
        int afterDot = afterDotByte - '0';

        decimal debug = (result + (afterDot / 10m)) * negativityMultiplier;
        return debug;
    }

    [Benchmark]
    [WarmupCount(2)]
    [IterationCount(5)]
    public decimal ParseDecimalUsingCopyPastedParser()
    {
        return CopyPastedParseTemperature(_line);
    }

    #region CopyPastedMagic
    private static int CopyPastedParseTemperature(ReadOnlySpan<char> input)
    {
        const byte dashChar = (byte)'-';
        const byte dotChar = (byte)'.';
        const byte zeroChar = (byte)'0';

        ref char firstByte = ref Unsafe.AsRef(in input[0]);

        int tempLength = 0;
        bool isNeg = firstByte == dashChar;
        if (isNeg)
        {
            tempLength++;
            firstByte = ref Unsafe.Add(ref firstByte, tempLength);
        }

        int temp = 0;
        while (firstByte != dotChar)
        {
            tempLength++;
            temp = temp * 10 + (firstByte - zeroChar);
            firstByte = ref Unsafe.Add(ref firstByte, 1);
        }

        char dec = input[tempLength + 1];
        temp = temp * 10 + (dec - zeroChar);

        if (isNeg)
        {
            temp = -temp;
        }

        return temp;
    }
    #endregion
}