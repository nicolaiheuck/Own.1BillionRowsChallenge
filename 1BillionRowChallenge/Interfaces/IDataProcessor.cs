﻿using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataProcessor
{
    List<ResultRow> ProcessData(string fileData);
}