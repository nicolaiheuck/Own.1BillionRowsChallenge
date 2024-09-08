using System.Globalization;

namespace _1BillionRowChallenge;

public class DataProcessor(string filePath)
{
    public List<ResultRow> ProcessData()
    {
        List<string> data = ReadDataFromFile();
        IEnumerable<DataPoint> dataPoints = ParseDataFromFile(data);
        List<ResultRow> aggregatedDataPoints = AggregateDataPoints(dataPoints);
        return aggregatedDataPoints;
    }

    private List<ResultRow> AggregateDataPoints(IEnumerable<DataPoint> dataPoints)
    {
        List<ResultRow> result = new();
        List<IGrouping<string, DataPoint>> group = dataPoints.GroupBy(d => d.CityName).ToList();

        foreach (IGrouping<string, DataPoint> dataPoint in group)
        {
            ResultRow row = new(dataPoint.Key)
            {
                Min = dataPoint.Min(d => d.Temperature),
                Max = dataPoint.Max(d => d.Temperature),
                Mean = dataPoint.Average(d => d.Temperature),
            };

            result.Add(row);
        }
        
        return result;
    }

    private IEnumerable<DataPoint> ParseDataFromFile(List<string> lines)
    {
        foreach (string line in lines)
        {
            string[] splitValue = line.Split(";");
            string cityName = splitValue[0];
            string temperatureAsString = splitValue[1];
            decimal temperature = decimal.Parse(temperatureAsString, CultureInfo.InvariantCulture);
            yield return new(cityName, temperature);
        }
    }

    private List<string> ReadDataFromFile()
    {
        return File.ReadAllLines(filePath).ToList();
    }
}
public record DataPoint(string CityName, decimal Temperature)
{
    // public string CityName { get; set; } = cityName;
    //
    // public decimal Temperature { get; set; } = temperature;
}
public class ResultRow(string cityName)
{
    public string CityName { get; set; } = cityName;

    public decimal? Min { get; set; }

    public decimal? Mean { get; set; }

    public decimal? Max { get; set; }
}