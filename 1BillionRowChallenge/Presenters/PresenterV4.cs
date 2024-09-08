using System.Collections;
using System.Globalization;
using System.Text;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge.Presenters;

public class PresenterV4 : IPresenterV4
{
    public string BuildResultString(List<ResultRowV4> data)
    {
        data.Sort((x, y) => string.Compare(x.CityName, y.CityName, StringComparison.Ordinal));
        StringBuilder builder = new();
        builder.Append('{');

        foreach (ResultRowV4 row in data)
        {
            string min = FixStupidNegativeZero(row.Min.ToString("F1", CultureInfo.InvariantCulture));
            string mean = FixStupidNegativeZero(row.Mean.ToString("F1", CultureInfo.InvariantCulture));
            string max = FixStupidNegativeZero(row.Max.ToString("F1", CultureInfo.InvariantCulture));
            builder.Append($"{row.CityName}={min}/{mean}/{max}");

            bool last = data.IndexOf(row) == data.Count - 1;
            if (!last)
            {
                builder.Append(", ");
            }
        }
        builder.Append('}');

        return builder.ToString();
    }

    private string FixStupidNegativeZero(string value)
    {
        if (value == "-0.0")
        {
            return "0.0";
        }

        return value;
    }
}