using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Interfaces;
using _1BillionRowChallenge.Models;
using _1BillionRowChallenge.Presenters;
using _1BillionRowChallenge.Processors;
using BenchmarkDotNet.Attributes;

namespace _1BillionRowChallenge;

public class DataProcessorBenchmark
{
    private string _fileDataForMeasurements10 = null!;
    private string _fileDataForMeasurements20 = null!;
    private string _fileDataForMeasurements10_000 = null!;
    private string _fileDataForMeasurements100_000 = null!;
    private string _fileDataForMeasurements1_000_000 = null!;
    private string _fileDataForMeasurements10_000_000 = null!;
    private string _fileDataForMeasurements1_000_000_000 = null!;
    private readonly IDataProcessor _dataProcessorV1 = new DataProcessorV1();
    private readonly IPresenter _presenter = new PresenterV1();
    // private DataProcessorV2 dataProcessorV2;

    [GlobalSetup]
    public void Setup()
    {
        _fileDataForMeasurements10 = File.ReadAllText(FilePathConstants.Measurements10);
        _fileDataForMeasurements20 = File.ReadAllText(FilePathConstants.Measurements20);
        _fileDataForMeasurements10_000 = File.ReadAllText(FilePathConstants.Measurements10_000);
        _fileDataForMeasurements100_000 = File.ReadAllText(FilePathConstants.Measurements100_000);
        _fileDataForMeasurements1_000_000 = File.ReadAllText(FilePathConstants.Measurements1_000_000);
        _fileDataForMeasurements10_000_000 = File.ReadAllText(FilePathConstants.Measurements10_000_000);
        _fileDataForMeasurements1_000_000_000 = File.ReadAllText(FilePathConstants.Measurements1_000_000_000);
    }

    // [Benchmark]
    // public void V1_10Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements10);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements10) throw new Exception("Incorrect hash");
    // }
    //
    // [Benchmark]
    // public void V1_20Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements20);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements20) throw new Exception("Incorrect hash");
    // }
    //
    // [Benchmark]
    // [IterationCount(5)]
    // [WarmupCount(2)]
    // public void V1_10_000Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements10_000);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements10_000) throw new Exception("Incorrect hash");
    // }
    //
    // [Benchmark]
    // [IterationCount(5)]
    // [WarmupCount(2)]
    // public void V1_100_000Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements100_000);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements100_000) throw new Exception("Incorrect hash");
    // }

    // [Benchmark]
    // [IterationCount(5)]
    // [WarmupCount(2)]
    // public void V1_1_000_000Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements1_000_000);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements1_000_000) throw new Exception("Incorrect hash");
    // }
    //
    // [Benchmark]
    // [IterationCount(5)]
    // [WarmupCount(2)]
    // public void V1_10_000_000Measurements()
    // {
    //     List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements10_000_000);
    //     string result = _presenter.BuildResultString(resultRows);
    //     if (Hasher.Hash(result) != CorrectHashes.Measurements10_000_000) throw new Exception("Incorrect hash");
    // }

    [Benchmark]
    [IterationCount(5)]
    [WarmupCount(2)]
    public void V1_1_000_000_000Measurements()
    {
        List<ResultRow> resultRows = _dataProcessorV1.ProcessData(_fileDataForMeasurements1_000_000_000);
        string result = _presenter.BuildResultString(resultRows);
        if (Hasher.Hash(result) != CorrectHashes.Measurements1_000_000_000) throw new Exception("Incorrect hash");
    }
}