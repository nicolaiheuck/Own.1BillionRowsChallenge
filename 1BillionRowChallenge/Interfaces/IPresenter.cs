using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge.Interfaces;

public interface IPresenter
{
    string BuildResultString(List<ResultRow> data);
}