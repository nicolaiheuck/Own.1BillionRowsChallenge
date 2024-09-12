namespace _1BillionRowChallenge.Models;

public class Block
{
    public Guid Id { get; set; } = Guid.NewGuid();
    
    public long Start { get; set; }

    public long End { get; set; }

    public string Debug1DisplayStart => $"{Start:N0}";
    public string Debug2DisplayEnd => $"{End:N0}";

    public string Debug3DisplayBlockSize => $"{End - Start:N0}";
}