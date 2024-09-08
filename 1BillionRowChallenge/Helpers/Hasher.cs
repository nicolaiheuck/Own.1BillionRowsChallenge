using System.Security.Cryptography;
using System.Text;

namespace _1BillionRowChallenge.Helpers;

public static class Hasher
{
    public static string Hash(string input)
    {
        byte[] plaintext = Encoding.UTF8.GetBytes(input);
        byte[] resultBytes = SHA256.HashData(plaintext);
        return string.Join("", resultBytes.Select(c => c.ToString("x2")));
    }
}