using System.Collections.Concurrent;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V6:
///   - Hopefully:
///     - Better single core performance
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | __ ms          |                                                   |
/// | 10,000        | _ ms           |                                                   |
/// | 100,000       | __ ms          |                                                   |
/// | 1,000,000     | ___ ms         |                                                   |
/// | 10,000,000    | ____ ms        |                                                   |
/// | 1,000,000,000 | ______ ms      |  _________ (40.6s AOT)                              |
/// Only 20 MB of memory
/// 22.2M rows a second (24M using AOT!!!)
/// </summary>
//NH_TODO: For next versions
// - Read file using pointers and unsafe
// - Try to maximize single thread performance and see how it scales
// - Split last block into 10 blocks and process them in parallel
// - Try using .ReadLines().Parallel()
// - Struct for AggregatedDataPointV5
public class DataStreamProcessorV8 : IDataStreamProcessorV5
{
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    //public static ConcurrentDictionary<int, ThreadProgressState> CurrentThreadState = new(); // Cost: 1.9M (aavaaaavv)
    public static long LinesProcessed = 0;
    private static List<Block> _blocks = [];

    public Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount, int? amountOfTasksInTotalOverwrite = null)
    {
        int amountOfTasksToRunInParallel = GetAmountOfTasksToRunInParallel(amountOfTasksInTotalOverwrite);
        _result = new();
        LinesProcessed = 0;
        //CurrentThreadState = new();
        Block.GlobalId = 0;

        // _blocks = FileSplitter.SplitFileIntoBlocks(filePath, amountOfTasksToRunInParallel);
        // using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(filePath);
        // List<Task> tasks = new();

        // Parallel.ForEach(File.ReadLines(filePath), new() { MaxDegreeOfParallelism = 9 }, (line, _) =>
        // {
        //     ReadOnlySpan<char> lineAsSpan = line.AsSpan();
        //     int indexOfSeparator = lineAsSpan.IndexOf(';');
        //
        //     ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
        //     ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
        //     decimal temperature = MyDecimalParserV9(temperatureSpan.ToString());
        //     AggregateRow((cityNameSpan.ToString(), (int)temperature));
        // });
        
        
        const int distributeOnThreads = 10;
        const int chunkSize = 100_000;
        List<TestBlock> blocks = FileSplitter.SplitFileIntoTestBlocks(filePath, 1_000_000_000 / chunkSize);
        using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(filePath);

        int blocksPerThread = blocks.Count / distributeOnThreads;
        List<TestBlock[]> chunksToProcess = blocks.Chunk(blocksPerThread).ToList();
        
        List<Task> tasks = new();
        foreach (TestBlock[] chunk in chunksToProcess)
        {
            tasks.Add(Task.Run(() => ProcessChunk(memoryMappedFile, chunk)));
        }
        
        Task.WaitAll(tasks.ToArray());
        

        // Stopwatch programStopWatch = Stopwatch.StartNew();
        // try
        // {
        //     await StartAsync(args);
        // }
        // finally
        // {
        //     programStopWatch.Stop();
        //     Console.WriteLine($"\nProgram finished in {programStopWatch.Elapsed.TotalSeconds:N1}s");
        // }
        return Task.FromResult(SecondLayerAggregation());
    }

    private unsafe Task ProcessChunk(MemoryMappedFile memoryMappedFile, TestBlock[] chunk)
    {
        foreach (TestBlock block in chunk)
        {
            MemoryMappedViewStream viewStream = memoryMappedFile.CreateViewStream(block.Start, block.End - block.Start, MemoryMappedFileAccess.Read);
            BufferedStream bufferedStream = new(viewStream, (int)(block.End - block.Start));
            StreamReader reader = new(bufferedStream);;
            string? line = reader.ReadLine();
            do
            {
                ReadOnlySpan<char> lineAsSpan = line.AsSpan();
                int indexOfSeparator = lineAsSpan.IndexOf(';');
                
                ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
                ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
                
                fixed (char* linePointer = temperatureSpan)
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
        
                    int temperature = (result + afterDot) * isNegativityMultiplier;
                    AggregatedDataPointV5 aggregatedDataPoint;
                    string cityName = cityNameSpan.ToString();
                    
                    if (_result.TryGetValue(cityName, out AggregatedDataPointV5? value))
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

                        _result[cityName] = aggregatedDataPoint;
                    }

                    if (temperature < aggregatedDataPoint.Min)
                    {
                        aggregatedDataPoint.Min = temperature;
                    }

                    if (temperature > aggregatedDataPoint.Max)
                    {
                        aggregatedDataPoint.Max = temperature;
                    }

                    Interlocked.Add(ref aggregatedDataPoint.Sum, temperature);
                    Interlocked.Increment(ref aggregatedDataPoint.AmountOfDataPoints);
                    Interlocked.Increment(ref LinesProcessed);
                
                    line = reader.ReadLine();
                }
            } while (line != null);
        }

        return Task.CompletedTask;
    }

    private static List<ResultRowV4> SecondLayerAggregation()
    {
        return _result.Select(keyPair => new ResultRowV4(keyPair.Key)
        {
            Min = keyPair.Value.Min / 10,
            Max = keyPair.Value.Max / 10,
            Mean = ((decimal)keyPair.Value.Sum / 10) / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    /// <summary>
    /// Saved 0.8 ns by using pointers (with fixed and unsafe)
    /// </summary>
    private unsafe static decimal MyDecimalParserV9(string line) // 1.803 ns
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

    private int GetAmountOfTasksToRunInParallel(int? amountOfTasksInTotalOverwrite)
    {
        // return 1; // Debugging
        
        // AOT
        // return 12; // 24M for 1B
        // return 24; // 22M for 1B
        // return 6; // 19M for 1B
        // return 11; // 25M, 24M
        // return 10; // 23.1M, 23.7M
        // return 9; // 23M for 1B
        // return 8; // 22M
        return 12; // Best
    }
    // private async Task AggregateRows(IEnumerable<(string, int)> rows, int taskId)
    // {
    //     foreach ((string? cityName, int temperature) in rows)
    //     {
    //         AggregatedDataPointV5 aggregatedDataPoint;
    //
    //         if (_result.TryGetValue(cityName, out AggregatedDataPointV5? value))
    //         {
    //             aggregatedDataPoint = value;
    //         }
    //         else
    //         {
    //             aggregatedDataPoint = new()
    //             {
    //                 Min = int.MaxValue,
    //                 Max = int.MinValue,
    //             };
    //
    //             _result[cityName] = aggregatedDataPoint;
    //         }
    //
    //         if (temperature < aggregatedDataPoint.Min)
    //         {
    //             aggregatedDataPoint.Min = temperature;
    //         }
    //
    //         if (temperature > aggregatedDataPoint.Max)
    //         {
    //             aggregatedDataPoint.Max = temperature;
    //         }
    //
    //         Interlocked.Add(ref aggregatedDataPoint.Sum, temperature);
    //         Interlocked.Increment(ref aggregatedDataPoint.AmountOfDataPoints);
    //         Interlocked.Increment(ref LinesProcessed);
    //
    //         //CurrentThreadState[taskId].LinesProcessedSoFar++;
    //     }
    //     //CurrentThreadState[taskId].IsFinished = true;
    //     //CurrentThreadState[taskId].Stopwatch.Stop();
    // }
}