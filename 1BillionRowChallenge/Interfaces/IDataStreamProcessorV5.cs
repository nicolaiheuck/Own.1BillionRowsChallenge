using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataStreamProcessorV5
{
    Task<List<ResultRowV4>> ProcessData(string filePath, long rowCount);
}