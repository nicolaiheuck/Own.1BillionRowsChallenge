using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge;

public static class BenchmarkConfigurationFactory
{
    public static BenchmarkConfiguration Create(BenchmarkType type, int? rowCount = null)
    {
        return new()
        {
            RowCount = rowCount ?? GetRowCount(type),
            FilePath = GetFilePath(type),
            CorrectHash = GetCorrectHash(type)
        };
    }

    private static long GetRowCount(BenchmarkType type)
    {
        return type switch
        {
            BenchmarkType.Measurements10 => 10,
            BenchmarkType.Measurements20 => 20,
            BenchmarkType.Measurements10_000 => 10_000,
            BenchmarkType.Measurements100_000 => 100_000,
            BenchmarkType.Measurements1_000_000 => 1_000_000,
            BenchmarkType.Measurements10_000_000 => 10_000_000,
            BenchmarkType.Measurements1_000_000_000 => 1_000_000_000,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };
    }
    
    private static string GetFilePath(BenchmarkType type)
    {
        return type switch
        {
            BenchmarkType.Measurements10 => FilePathConstants.Measurements10,
            BenchmarkType.Measurements20 => FilePathConstants.Measurements20,
            BenchmarkType.Measurements10_000 => FilePathConstants.Measurements10_000,
            BenchmarkType.Measurements100_000 => FilePathConstants.Measurements100_000,
            BenchmarkType.Measurements1_000_000 => FilePathConstants.Measurements1_000_000,
            BenchmarkType.Measurements10_000_000 => FilePathConstants.Measurements10_000_000,
            BenchmarkType.Measurements1_000_000_000 => FilePathConstants.Measurements1_000_000_000,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };
    }
    
    private static string GetCorrectHash(BenchmarkType type)
    {
        return type switch
        {
            BenchmarkType.Measurements10 => CorrectHashes.Measurements10,
            BenchmarkType.Measurements20 => CorrectHashes.Measurements20,
            BenchmarkType.Measurements10_000 => CorrectHashes.Measurements10_000,
            BenchmarkType.Measurements100_000 => CorrectHashes.Measurements100_000,
            BenchmarkType.Measurements1_000_000 => CorrectHashes.Measurements1_000_000,
            BenchmarkType.Measurements10_000_000 => CorrectHashes.Measurements10_000_000,
            BenchmarkType.Measurements1_000_000_000 => CorrectHashes.Measurements1_000_000_000,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };
    }
}