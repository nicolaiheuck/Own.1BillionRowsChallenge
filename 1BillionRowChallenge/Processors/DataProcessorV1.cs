﻿using System.Globalization;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

public class DataProcessorV1 : IDataProcessor
{
    public List<ResultRow> ProcessData(string filePath)
    {
        List<string> data = ReadLines(filePath);
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

    private List<string> ReadLines(string fileData)
    {
        return fileData.Split("\n", StringSplitOptions.RemoveEmptyEntries).ToList();
    }
}