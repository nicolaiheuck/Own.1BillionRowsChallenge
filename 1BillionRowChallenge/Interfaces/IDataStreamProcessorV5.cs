using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataStreamProcessorV5
{
    List<ResultRowV5> ProcessData(string filePath);
}