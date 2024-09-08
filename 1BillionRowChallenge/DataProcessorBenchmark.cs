using System.Runtime.Intrinsics.Arm;
using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge;

public class DataProcessorBenchmark
{
    private string _fileData = null!;
    private DataProcessorV1 _dataProcessorV1 = new();
    private IPresenter presenter = new PresenterV1();
    // private DataProcessorV2 dataProcessorV2;

    [GlobalSetup]
    public void Setup()
    {
        const string filePath = @"C:\Users\Googlelai\Desktop\Nerd\1b-rows-challenge\1brc.data\measurements-10.txt";
        _fileData = File.ReadAllText(filePath);
        _dataProcessorV1 = new();
    }

    [Benchmark]
    public void BenchmarkVersion1()
    {
        List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileData);
        string result = presenter.BuildResultString(resultRows);
        if (Hasher.Hash(result) != CorrectHashes.Measurements10) throw new Exception("Incorrect hash");
    }
}