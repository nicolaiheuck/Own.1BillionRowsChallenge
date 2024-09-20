namespace _1BillionRowChallenge.Helpers;

public static class ConsoleHelper
{
    private static readonly object _consoleLock = new();
    public static void WriteLine(string text)
    {
        lock (_consoleLock)
        {
            Console.WriteLine(text);
        }
    }
    
    public static void ColoredWriteLine(string text, ConsoleColor color, int? left = null, int? top = null)
    {
        lock (_consoleLock)
        {
            if (left != null && top != null)
            {
                int originalLeft = Console.CursorLeft;
                int originalTop = Console.CursorTop;
                Console.SetCursorPosition(left.Value, top.Value);
                
                Console.ForegroundColor = color;
                Console.WriteLine(text);
                Console.ResetColor();
                
                Console.SetCursorPosition(originalLeft, originalTop >= Console.WindowHeight - 1 ? originalTop : originalTop + 1);
            }
            else
            {
                Console.ForegroundColor = color;
                Console.WriteLine(text);
                Console.ResetColor();
            }
        }
    }
    
    public static void WriteAtPosition(int left, int top, string text)
    {
        lock (_consoleLock)
        {
            int originalLeft = Console.CursorLeft;
            int originalTop = Console.CursorTop;
            Console.SetCursorPosition(left, top);
            Console.Write(text);
            Console.SetCursorPosition(originalLeft, originalTop);
        }
    }
}