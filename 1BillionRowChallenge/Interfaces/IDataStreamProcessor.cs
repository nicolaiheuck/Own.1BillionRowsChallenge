using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataStreamProcessor
{
    List<ResultRow> ProcessData(string filePath);
}