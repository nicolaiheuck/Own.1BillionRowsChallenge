using _1BillionRowChallenge.Helpers;
using _1BillionRowChallenge.Models;

namespace _1BillionRowChallenge.Processors;

public class FileSplitter
{
    public static List<Block> SplitFileIntoBlocks(string filePath, int blockCount, long offset = 0)
    {
        if (blockCount == 1) return [new() { Start = offset, End = new FileInfo(filePath).Length }];
        
        FileInfo fileInfo = new(filePath);
        using FileStream fileStream = File.OpenRead(filePath);
        using StreamReader reader = new(fileStream);
        List<Block> blocks = [];
        
        long blockSize = (fileInfo.Length - offset) / blockCount;
        const char seperator = '\n';
        for (int i = 0; i < blockCount; i++)
        {
            long start = i * blockSize + offset;
            Block? lastBlock = blocks.LastOrDefault();

            if (lastBlock != null && lastBlock.End > start)
            {
                start = lastBlock.End;
            }
            long end = (i + 1) * blockSize + offset;
            fileStream.Position = end;
            ReadToNextSeperator(seperator, fileStream);
            end = fileStream.Position;
            blocks.Add(new() { Start = start, End = end });
        }
        return blocks;
    }

    private static void ReadToNextSeperator(char seperator, FileStream stream)
    {
        int readByte;

        do
        {
            readByte = stream.ReadByte();

            if (readByte == -1)
            {
                Console.WriteLine("End of file reached while searching for seperator. This should not happen.");
                break;
            }
        } while (readByte != seperator);
    }
}