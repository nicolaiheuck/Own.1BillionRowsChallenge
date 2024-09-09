using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IPresenterV5
{
    string BuildResultString(List<ResultRowV5> data);
}