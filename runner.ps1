for ($i = 10; $i -le 1000; $i += 10) {
    echo "Running with $i threads";
    $startTime = Get-Date
    dotnet .\1BillionRowChallenge\bin\Debug\net8.0\1BillionRowChallenge.dll $i >> ./output.txt
    $endTime = Get-Date
    $duration = $endTime - $startTime
    echo "Execution time: $duration"
}