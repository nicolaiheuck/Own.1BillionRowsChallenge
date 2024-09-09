namespace _1BillionRowChallenge.Models;

public class ResultRowV5(ushort cityName)
{
    public ushort CityName { get; set; } = cityName;

    public decimal Min { get; set; }

    public decimal Mean { get; set; }

    public decimal Max { get; set; }
}