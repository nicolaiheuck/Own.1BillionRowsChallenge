using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IPresenter
{
    string BuildResultString(List<ResultRow> data);
}