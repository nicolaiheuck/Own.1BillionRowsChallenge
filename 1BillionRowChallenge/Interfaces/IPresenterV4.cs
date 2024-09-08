using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Processors;

namespace _1BillionRowChallenge.Interfaces;

public interface IPresenterV4
{
    string BuildResultString(List<ResultRowV4> data);
}