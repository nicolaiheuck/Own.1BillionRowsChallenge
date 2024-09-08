using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataStreamProcessorV4
{
    List<ResultRowV4> ProcessData(string filePath);
}