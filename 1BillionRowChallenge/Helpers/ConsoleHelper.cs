﻿namespace _1BillionRowChallenge.Helpers;

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
    
    public static void ColoredWriteLine(string text, ConsoleColor color, int left = 0, int top = 0)
    {
        lock (_consoleLock)
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