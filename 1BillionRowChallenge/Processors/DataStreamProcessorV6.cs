using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Reflection.Metadata;
using System.Text;
using _1BillionRowChallenge.Helpers;
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
    public Stopwatch Stopwatch { get; set; } = new();
    public long LinesProcessedSoFar { get; set; }
    public long LinesToProcess { get; set; }
    public bool IsFinished { get; set; }
    public long BytesReadSoFar { get; set; }
    public long BytesToRead { get; set; }
}
public class DataStreamProcessorV6 : IDataStreamProcessorV5
{
    private static int _linesProcessed;
    private static ConcurrentDictionary<string, AggregatedDataPointV5> _result = new();
    public readonly static ConcurrentDictionary<Guid, ThreadProgressState> CurrentThreadState = new();
    private static SemaphoreSlim _semaphore = null!;
    private static List<Block> _blocks = [];
    
    public async Task<List<ResultRowV4>> ProcessData(string filePath, long rowCountOld, int? amountOfTasksInTotalOverwrite = null)
    {
        const int amountOfTasksToRunInParallel = 5;
        _semaphore = new(amountOfTasksToRunInParallel, amountOfTasksToRunInParallel);
        _linesProcessed = 0;
        _result = new();

        TaskFactory taskFactory = new(TaskCreationOptions.RunContinuationsAsynchronously, TaskContinuationOptions.ExecuteSynchronously);
        _blocks = SplitFileIntoBlocks(filePath, 10);
        using MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateFromFile(filePath);
        List<Task> tasks = [];
        foreach (Block block in _blocks.Take(5))
        {
            tasks.Add(taskFactory.StartNew(async () =>
            {
                try
                {
                    const int rowCount = 100_000_000;
                    CurrentThreadState[block.Id] = new() { LinesToProcess = rowCount, BytesToRead = block.End - block.Start };
                    using MemoryMappedViewStream viewStream = memoryMappedFile.CreateViewStream(block.Start, block.End - block.Start, MemoryMappedFileAccess.Read);
                    IEnumerable<(string, int)> rows = ReadRowsFromFile(viewStream , block);
                    await AggregateRows(rows, block.Id, rowCount);
                }
                catch (Exception ex)
                {
                    ConcurrentConsoleHelperDecorator.WriteAtPosition(0, 10, $"Error while running task {block.Id}:");
                    ConcurrentConsoleHelperDecorator.WriteAtPosition(0, 11, ex.Message);
                    throw;
                }
            }));
        }
        await Task.WhenAll(tasks);
        Console.Clear();
        ConcurrentConsoleHelperDecorator.WriteAtPosition(0, 10, "First pass done. Press a key to continue");
        Console.ReadKey();
        return [];

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
        // return SecondLayerAggregation();
    }

    private static async Task AggregateRows(IEnumerable<(string, int)> rows, Guid taskId, long rowCount)
    {
        await _semaphore.WaitAsync();

        long i = 0;
        
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

                CurrentThreadState[taskId].LinesProcessedSoFar++;
                // if (i % 10_000_000 == 0)
                // {
                //     return;
                //     //     // ConsoleHelper.Write($"\rAggregated {_linesProcessed:N0} rows");
                // }
            }
        }
        finally
        {
            _semaphore.Release();
            CurrentThreadState[taskId].IsFinished = true;
        }
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

    private static IEnumerable<(string, int)> ReadRowsFromFile(MemoryMappedViewStream viewStream, Block block)
    {
        CurrentThreadState[block.Id].Stopwatch.Start();
        using StreamReader reader = new(viewStream);
        
        string? line = reader.ReadLine();
        do
        {
            CurrentThreadState[block.Id].BytesReadSoFar += line?.Length ?? 0;

            ReadOnlySpan<char> lineAsSpan = line.AsSpan();
            int indexOfSeparator = lineAsSpan.IndexOf(';');

            ReadOnlySpan<char> cityNameSpan = lineAsSpan.Slice(0, indexOfSeparator);
            ReadOnlySpan<char> temperatureSpan = lineAsSpan.Slice(indexOfSeparator + 1);
            decimal temperature = decimal.Parse(temperatureSpan, CultureInfo.InvariantCulture) * 100;
            yield return (cityNameSpan.ToString(), (int)temperature);
            line = reader.ReadLine();
        } while (line != null);
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
                ConcurrentConsoleHelperDecorator.WriteLine("End of file reached while searching for seperator. This should not happen.");
                break;
            }
        } while (readByte != seperator);
    }

    // private static void BoundaryTest(string filePath, long startOfBoundary, long endOfBoundary)
    // {
    //     using FileStream fileStream = File.OpenRead(filePath);
    //     
    //     fileStream.Position = startOfBoundary;
    //     char firstByte = (char)fileStream.ReadByte();
    //     
    //     fileStream.Position = endOfBoundary;
    //     char lastByte = (char)fileStream.ReadByte();
    //     
    //     fileStream.Position = endOfBoundary -1;
    //     char secondLastByte = (char)fileStream.ReadByte();
    //     
    //     ConcurrentConsoleHelperDecorator.WriteLine($"\tFrom {firstByte} to {lastByte}");
    //     if (secondLastByte != '\n') {
    //         ConcurrentConsoleHelperDecorator.WriteLine($"Second last byte is not a newline (from {startOfBoundary} to {endOfBoundary})");
    //     }
    // }
    //
    // private static string ReadBytes(FileStream fileStream, int bytesToRead)
    // {
    //     string result = "";
    //     for (int i = 0; i < bytesToRead; i++)
    //     {
    //         char readByte = (char)fileStream.ReadByte();
    //         result += readByte;
    //     }
    //
    //     return result;
    // }
}
public class Block
{
    public Guid Id { get; set; } = Guid.NewGuid();
    
    public long Start { get; set; }

    public long End { get; set; }

    public string Debug1DisplayStart => $"{Start:N0}";
    public string Debug2DisplayEnd => $"{End:N0}";

    public string Debug3DisplayBlockSize => $"{End - Start:N0}";
}