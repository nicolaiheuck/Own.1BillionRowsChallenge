namespace _1BillionRowChallenge.Models;

public class ResultRowV4(string cityName)
{
    public string CityName { get; set; } = cityName;

    public double Min { get; set; }

    public double Mean { get; set; }

    public double Max { get; set; }
}