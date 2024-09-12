using System.Diagnostics;

namespace _1BillionRowChallenge.Models;

public class ThreadProgressState
{
    public Stopwatch Stopwatch { get; set; } = new();
    public long LinesProcessedSoFar { get; set; }
    public long LinesToProcess { get; set; }
    public bool IsFinished { get; set; }
    public long BytesReadSoFar { get; set; }
    public long BytesToRead { get; set; }
}