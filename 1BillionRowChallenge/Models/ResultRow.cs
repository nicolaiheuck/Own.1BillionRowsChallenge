namespace _1BillionRowChallenge.Models;

public class ResultRow(string cityName)
{
    public string CityName { get; set; } = cityName;

    public decimal? Min { get; set; }

    public decimal? Mean { get; set; }

    public decimal? Max { get; set; }
}