using System.Diagnostics;

namespace _1BillionRowChallenge.Helpers;

public static class TimeLogger
{
    public static void LogExecution(string description, Action action, long? rowCount = null)
    {
        Stopwatch watch = new();

        try
        {
            watch.Start();
            action();
        }
        finally
        {
            watch.Stop();
            Console.WriteLine($"Execution time for \"{description}\" in {watch.ElapsedMilliseconds}ms ({rowCount/watch.Elapsed.TotalSeconds:N0} rows/sec)");
        }
    }
    public static async Task<long> LogExecutionAsync(string description, Func<Task> action, long? rowCount = null)
    {
        Stopwatch watch = new();

        try
        {
            watch.Start();
            await action();
        }
        finally
        {
            watch.Stop();
            Console.WriteLine($"Execution time for \"{description}\" in {watch.ElapsedMilliseconds}ms ({rowCount/watch.Elapsed.TotalSeconds:N0} rows/sec)");
        }
        return watch.ElapsedMilliseconds;
    }
}