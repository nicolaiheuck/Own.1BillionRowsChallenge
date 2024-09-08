using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge.Interfaces;

public interface IDataProcessor
{
    List<ResultRow> ProcessData(string fileData);
}