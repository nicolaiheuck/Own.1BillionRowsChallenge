namespace _1BillionRowChallenge.Helpers;

public static class ConcurrentConsoleHelperDecorator
{
    private static readonly object _consoleLock = new();
    public static void WriteLine(string text)
    {
        lock (_consoleLock)
        {
            ConsoleHelper.WriteLine(text);
        }
    }
    
    public static void ColoredWriteLine(string text, ConsoleColor color, int left = 0, int top = 0)
    {
        lock (_consoleLock)
        {
            ConsoleHelper.ColoredWriteLine(text, color, left, top);
        }
    }
    
    public static void WriteAtPosition(int left, int top, string text)
    {
        lock (_consoleLock)
        {
            ConsoleHelper.WriteAtPosition(left, top, text);
        }
    }
}
public static class ConsoleHelper
{
    public static void WriteLine(string text)
    {
        Console.WriteLine(text);
    }
    
    public static void WriteAtPosition(int left, int top, string text)
    {
        int originalLeft = Console.CursorLeft;
        int originalTop = Console.CursorTop;
        Console.SetCursorPosition(left, top);
        Console.Write(text);
        Console.SetCursorPosition(originalLeft, originalTop);
    }

    public static void ColoredWriteLine(string text, ConsoleColor color, int left, int top)
    {
        int originalLeft = Console.CursorLeft;
        int originalTop = Console.CursorTop;
        Console.SetCursorPosition(left, top);
        Console.ForegroundColor = color;

        Console.WriteLine(text);
        
        Console.ResetColor();
        Console.SetCursorPosition(originalLeft, originalTop);
    }
}