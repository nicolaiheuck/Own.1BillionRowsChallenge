﻿using System.Globalization;
using System.Text;

namespace _1BillionRowChallenge;

public class Presenter(List<ResultRow> data)
{
    public string BuildResultString()
    {
        StringBuilder builder = new();
        builder.Append('{');
        var orderedData = data.OrderBy(d => d.CityName).ToList();

        foreach (ResultRow row in orderedData)
        {
            //{Adelaide=15.0/15.0/15.0, Cabo San Lucas=14.9/14.9/14.9, Dodoma=22.2/22.2/22.2, Halifax=12.9/12.9/12.9, Karachi=15.4/15.4/15.4, Pittsburgh=9.7/9.7/9.7, Ségou=25.7/25.7/25.7, Tauranga=38.2/38.2/38.2, Xi'an=24.2/24.2/24.2, Zagreb=12.2/12.2/12.2}

            string min = row.Min?.ToString("F1", CultureInfo.InvariantCulture)!;
            string mean = row.Mean?.ToString("F1", CultureInfo.InvariantCulture)!;
            string max = row.Max?.ToString("F1", CultureInfo.InvariantCulture)!;
            builder.Append($"{row.CityName}={min}/{mean}/{max}");

            bool last = orderedData.IndexOf(row) == data.Count - 1;
            if (!last)
            {
                builder.Append(", ");
            }
        }
        builder.Append('}');

        return builder.ToString();
    }
}