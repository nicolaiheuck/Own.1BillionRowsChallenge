using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

/// <summary>
/// Changes form V5:
/// 
/// Benchmarks:
/// | File Size     | Execution Time | Rows per Second                                   |
/// |---------------|----------------|---------------------------------------------------|
/// | 10            | __ ms          |                                                   |
/// | 10,000        | _ ms           |                                                   |
/// | 100,000       | __ ms          |                                                   |
/// | 1,000,000     | ___ ms         |                                                   |
/// | 10,000,000    | ____ ms        |                                                   |
/// | 1,000,000,000 | ______ ms      |  _________ (____ minutes or ____ using AOT)       |
/// Only __ MB of memory
/// ___M rows a second (___M using AOT)
/// </summary>
//NH_TODO: For next versions
//             Split processing into a first and second pass
//             MemoryMappedFile.CreateFromFile
public class DataStreamProcessorV6 : IDataStreamProcessorV5
{
    private static int _linesProcessed;
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    private static SemaphoreSlim _semaphore;
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount, int? amountOfTasksInTotalOverwrite = null)
    {
        _linesProcessed = 0;
        _result = new();

        List<Block> blocks = SplitFileIntoBlocks(filePath, 10);
        Console.WriteLine("Planning to read:");
        foreach (Block block in blocks.Take(9))
        {
            Console.WriteLine($"\tFrom {block.Start} to {block.End}");
            BoundaryTest(filePath, block.Start, block.End);
        }

        Block lastBlock = blocks.Last();
        Console.WriteLine($"Second pass planning to read (from {lastBlock.Start} to {lastBlock.End}):");
        
        blocks = SplitFileIntoBlocks(filePath, 10, lastBlock.Start);
        foreach (Block block in blocks)
        {
            Console.WriteLine($"\tFrom {block.Start} to {block.End}");
            BoundaryTest(filePath, block.Start, block.End);
        }

        // const int amountOfTasksInTotalConst = 10;
        // const int amountOfTasksToRunInParallel = 10;
        // _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        // int amountOfTasksInTotal = amountOfTasksInTotalOverwrite ?? amountOfTasksInTotalConst;
        // List<Task> tasks = [];
        //
        // int taskId = 0;
        // for (int i = 0; i < amountOfTasksInTotal; i++)
        // {
        //     int startAtLine = (int)(i * (rowCount / amountOfTasksInTotal));
        //     int stopAtLine = (int)((i + 1) * (rowCount / amountOfTasksInTotal));
        //     tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, startAtLine, stopAtLine), taskId++)));
        // }
        //
        // await Task.WhenAll(tasks);
        // return SecondLayerAggregation();
        return [];
    }

    private void BoundaryTest(string filePath, long startOfBoundary, long endOfBoundary)
    {
        using FileStream fileStream = File.OpenRead(filePath);
        const int sectionWidth = 3;
        
        fileStream.Position = startOfBoundary;
        char firstByte = (char)fileStream.ReadByte();
        
        fileStream.Position = endOfBoundary;
        char lastByte = (char)fileStream.ReadByte();
        
        fileStream.Position = endOfBoundary -1;
        char secondLastByte = (char)fileStream.ReadByte();
        
        Console.WriteLine($"\tFrom {firstByte} to {lastByte}");
        if (secondLastByte != '\n') {
            Console.WriteLine($"Second last byte is not a newline (from {startOfBoundary} to {endOfBoundary})");
        }
    }

    private string ReadBytes(FileStream fileStream, int bytesToRead)
    {
        string result = "";
        for (int i = 0; i < bytesToRead; i++)
        {
            char readByte = (char)fileStream.ReadByte();
            result += readByte;
        }

        return result;
    }

    private static List<Block> SplitFileIntoBlocks(string filePath, int blockCount, long offset = 0)
    {
        FileInfo fileInfo = new(filePath);
        using FileStream fileStream = File.OpenRead(filePath);
        using StreamReader reader = new(fileStream);
        List<Block> blocks = [];
        
        long blockSize = (fileInfo.Length - offset) / blockCount;
        const char seperator = '\n';
        for (int i = 0; i < blockCount; i++)
        {
            long start = i * blockSize + offset;
            Block? lastBlock = blocks.LastOrDefault();

            if (lastBlock != null && lastBlock.End > start)
            {
                start = lastBlock.End + 1;
            }
            long end = (i + 1) * blockSize + offset;
            fileStream.Position = end;
            ReadToNextSeperator(seperator, fileStream);
            end = fileStream.Position;
            blocks.Add(new() { Start = start, End = end });
        }
        return blocks;
    }

    private static void ReadToNextSeperator(char seperator, FileStream stream)
    {
        int readByte;

        do
        {
            readByte = stream.ReadByte();
        } while (readByte != seperator);
    }

    private static List<ResultRowV4> SecondLayerAggregation()
    {
        return _result.Select(keyPair => new ResultRowV4(keyPair.Key)
        {
            Min = keyPair.Value.Min / 100,
            Max = keyPair.Value.Max / 100,
            Mean = ((decimal)keyPair.Value.Sum / 100) / keyPair.Value.AmountOfDataPoints
        }).ToList();
    }

    private static void AggregateRows(IEnumerable<(string, int)> rows, int taskId)
    {
        _semaphore.Wait();

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

                Interlocked.Increment(ref _linesProcessed);

                if (_linesProcessed % 1_000_000 == 0)
                {
                    Console.Write($"\rAggregated {_linesProcessed:N0} rows");
                }
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static IEnumerable<ValueTuple<string, int>> ReadRowsFromFile(string filePath, int start, int end)
    {
        foreach (string line in File.ReadLines(filePath).Skip(start).Take(end - start))
        {
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture) * 100;
            yield return (cityNameSpan.ToString(), (int)temperature);
        }
    }
}
public class Block
{
    public long Start { get; set; }

    public long End { get; set; }
}