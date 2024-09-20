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
/// | 1,000,000,000 | ______ ms      |  _________ (44s or 56s using AOT)                 |
/// Only 20 MB of memory
/// 22.2M rows a second (17.9M using AOT)
/// </summary>
//NH_TODO: For next versions
// - Try to maximize single thread performance and see how it scales
// - Split last block into 10 blocks and process them in parallel
// - Try using .ReadLines().Parallel()
public class DataStreamProcessorV7 : IDataStreamProcessorV5
{
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    // public static ConcurrentDictionary<int, ThreadProgressState> CurrentThreadState = new(); // Cost: 236K
    private long LinesProcessed = 0;
    private static SemaphoreSlim _semaphore = null!; // Cost: 0
    private static List<Block> _blocks = [];
    
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount, int? amountOfTasksInTotalOverwrite = null)
    {
        EnsureFileExists(filePath);
        int amountOfTasksToRunInParallel = GetAmountOfTasksToRunInParallel(amountOfTasksInTotalOverwrite);
        _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        _result = new();
        LinesProcessed = 0;
      //  CurrentThreadState = new();

        _blocks = FileSplitter.SplitFileIntoBlocks(filePath, amountOfTasksToRunInParallel);
        using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(filePath);
        List<Task> tasks = [];

        await Parallel.ForEachAsync(_blocks, new ParallelOptions { MaxDegreeOfParallelism = amountOfTasksToRunInParallel }, async (block, cancellationToken) => // 44s with. Not sure if it helps or not
        {
            // await _semaphore.WaitAsync(cancellationToken);
            Console.Write($"\rLines: {LinesProcessed:N0}");
            //  CurrentThreadState[block.Id] = new() { LinesToProcess = rowCount / _blocks.Count, BytesToRead = block.End - block.Start };
            using MemoryMappedViewStream viewStream = memoryMappedFile.CreateViewStream(block.Start, block.End - block.Start, MemoryMappedFileAccess.Read);
            IEnumerable<(string, int)> rows = ReadRowsFromFile(viewStream, block);
            await AggregateRows(rows, block.Id);
        });
        // foreach (Block block in _blocks)
        // {
        //     tasks.Add(Task.Run(async () =>
        //     {
        //     }));
        // }
        // await Task.WhenAll(tasks); // Cost: 0

        // Block lastBlock = blocks.Last();
        //
        // Console.Clear();
        // ConsoleHelper.WriteLine("Press a key to start the second pass");
        // Console.ReadKey(true);
        // blocks = SplitFileIntoBlocks(filePath, 10, lastBlock.Start);
        // foreach (Block block in blocks)
        // {
        //     long rowCount = block.End - block.Start;
        //     tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, block.Start, block.End), taskId++, 10_000_000)));
        // }
        // await Task.WhenAll(tasks);
        //
        return SecondLayerAggregation();
    }

    private static void EnsureFileExists(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException("File not found", filePath);
        }
    }

    private int GetAmountOfTasksToRunInParallel(int? amountOfTasksInTotalOverwrite)
    {
        // return 60_000; // 10.9M
        // return 6_000; // 18.8M
        // return amountOfTasksInTotalOverwrite ?? 6_000;
        return 500;
    }

    private async Task AggregateRows(IEnumerable<(string, int)> rows, int taskId)
    {
        await _semaphore.WaitAsync();

        try
        {
            foreach ((string? cityName, int temperature) in rows)
            {
                AggregatedDataPointV5 aggregatedDataPoint;

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

                // CurrentThreadState[taskId].LinesProcessedSoFar++;
            }
        }
        finally
        {
            _semaphore.Release();
          //  CurrentThreadState[taskId].IsFinished = true;
          //  CurrentThreadState[taskId].Stopwatch.Stop();
        }
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

    private static IEnumerable<(string, int)> ReadRowsFromFile(MemoryMappedViewStream viewStream, Block block)
    {
      //  CurrentThreadState[block.Id].Stopwatch.Start();
        using StreamReader reader = new(viewStream);
        
        string? line = reader.ReadLine();
        do
        {
          //  CurrentThreadState[block.Id].BytesReadSoFar += line?.Length ?? 0;

            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = MyDecimalParserV9(temperatureSpan.ToString());
            yield return (cityNameSpan.ToString(), (int)temperature);
            line = reader.ReadLine();
        } while (line != null);
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
}