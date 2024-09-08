namespace _1BillionRowChallenge.Models;

public class AggregatedDataPoint
{
    public decimal? Min { get; set; }
    public decimal? Max { get; set; }
    public decimal Sum { get; set; }
    public decimal AmountOfDataPoints { get; set; }
}