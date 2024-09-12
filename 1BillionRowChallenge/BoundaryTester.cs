using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge;

public class BoundaryTester
{
    public static void BoundaryTest(string filePath, Block block)
    {
        using FileStream fileStream = File.OpenRead(filePath);
        string debugContent = File.ReadAllText(filePath);
        
        fileStream.Position = block.Start;
        char firstByte = (char)fileStream.ReadByte();
        
        fileStream.Position = block.End;
        char lastByte = (char)fileStream.ReadByte();
        
        
        string debugSection = debugContent.Substring((int)block.Start, (int)block.End - (int)block.Start);
        
        fileStream.Position = block.End -1;
        char secondLastByte = (char)fileStream.ReadByte();
        
        Console.WriteLine($"\tFrom {firstByte} to {lastByte}");
        if (secondLastByte != '\n') {
            Console.WriteLine($"Second last byte is not a newline (from {block.Start} to {block.End})");
        }
    }

    private static string ReadBytes(FileStream fileStream, int bytesToRead)
    {
        string result = "";
        for (int i = 0; i < bytesToRead; i++)
        {
            char readByte = (char)fileStream.ReadByte();
            result += readByte;
        }
    
        return result;
    }
}