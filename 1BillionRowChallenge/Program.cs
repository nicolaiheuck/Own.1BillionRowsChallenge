
namespace _1BillionRowChallenge;

public class Program
{
    public static void Main(string[] args)
    {
        const string filePath = @"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-10.txt";
        DataProcessor processor = new DataProcessor(filePath);
        List<ResultRow> processedData = processor.ProcessData();
        Presenter presenter = new Presenter(processedData);
        string result = presenter.BuildResultString();
        Console.WriteLine(result);
    }
}