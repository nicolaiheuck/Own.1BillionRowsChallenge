namespace _1BillionRowChallenge.Models;

public class Block
{
    private static int _globalId = 0;
    
    public int Id { get; set; }
    
    public long Start { get; set; }

    public long End { get; set; }

    public string Debug1DisplayStart => $"{Start:N0}";
    public string Debug2DisplayEnd => $"{End:N0}";

    public string Debug3DisplayBlockSize => $"{End - Start:N0}";

    public Block()
    {
        Id = Interlocked.Increment(ref _globalId);
    }
}