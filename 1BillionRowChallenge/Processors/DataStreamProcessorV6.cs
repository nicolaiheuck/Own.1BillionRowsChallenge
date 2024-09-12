using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
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
public class ThreadProgressState
{
    public long LinesProcessedSoFar { get; set; }
    public long LinesToProcess { get; set; }
}
public class DataStreamProcessorV6 : IDataStreamProcessorV5
{
    private static int _linesProcessed;
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    // public readonly static ConcurrentDictionary<int, ThreadProgressState> CurrentThreadState = new();
    private static SemaphoreSlim _semaphore = null!;
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCountOld, int? amountOfTasksInTotalOverwrite = null)
    {
        const int amountOfTasksToRunInParallel = 1;
        _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        _linesProcessed = 0;
        _result = new();

        List<Block> blocks = SplitFileIntoBlocks(filePath, 10);
        List<Task> tasks = [];
        int taskId = 0;
        foreach (Block block in blocks.Take(1))
        {
            tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, block.Start, block.End), taskId++, 100_000_000)));
        }
        await Task.WhenAll(tasks);
        return [];

        Block lastBlock = blocks.Last();

        Console.Clear();
        Console.WriteLine("Press a key to start the second pass");
        Console.ReadKey(true);
        blocks = SplitFileIntoBlocks(filePath, 10, lastBlock.Start);
        foreach (Block block in blocks)
        {
            long rowCount = block.End - block.Start;
            tasks.Add(Task.Run(() => AggregateRows(ReadRowsFromFile(filePath, block.Start, block.End), taskId++, 10_000_000)));
        }
        await Task.WhenAll(tasks);
        
        return SecondLayerAggregation();
    }

    private void BoundaryTest(string filePath, long startOfBoundary, long endOfBoundary)
    {
        using FileStream fileStream = File.OpenRead(filePath);
        
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

            if (readByte == -1)
            {
                Console.WriteLine("End of file reached while searching for seperator. This should not happen.");
                break;
            }
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

    private static void AggregateRows(IEnumerable<(string, int)> rows, int taskId, long rowCount)
    {
        _semaphore.Wait();

        long i = 0;
        // CurrentThreadState[taskId] = new() { LinesProcessedSoFar = 0, LinesToProcess = rowCount };
        
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
                i++;

                if (_linesProcessed % 1_000_000 == 0)
                {
                    Console.Write($"\rAggregated {_linesProcessed:N0} rows");
                    // CurrentThreadState[taskId].LinesProcessedSoFar = i;
                }
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static IEnumerable<ValueTuple<string, int>> ReadRowsFromFile(string filePath, long start, long end)
    { 
        using FileStream fileStream = File.OpenRead(filePath);
        fileStream.Position = start;
        long amountOfBytesToRead = end - start;
        byte[] buffer = new byte[amountOfBytesToRead];
        int read = fileStream.Read(buffer);
        using MemoryStream memoryStream = new(buffer);
        if (read == 0) throw new("Reached end of file. Shouldn't happen.");
        using StreamReader reader = new(memoryStream);

        int i = 0;

        string? line = "";
        do
        {
            line = reader.ReadLine();
            i++;
            Console.Write($"\rReading line {i:N0}");
            
            if (fileStream.Position > end)
            {
                break;
            }
            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture) * 100;
            yield return (cityNameSpan.ToString(), (int)temperature);
        } while (line != null);

        Console.WriteLine("Done reading. Press a key");
        Console.ReadKey();
    }
}
public class Block
{
    public long Start { get; set; }

    public long End { get; set; }
}