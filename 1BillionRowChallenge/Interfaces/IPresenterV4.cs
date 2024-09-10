using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Interfaces;

public interface IPresenterV4
{
    string BuildResultString(List<ResultRowV4> data);
}