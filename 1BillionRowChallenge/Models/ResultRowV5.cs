namespace _1BillionRowChallenge.Models;

public class ResultRowV5(short cityName)
{
    public short CityName { get; set; } = cityName;

    public decimal Min { get; set; }

    public decimal Mean { get; set; }

    public decimal Max { get; set; }
}