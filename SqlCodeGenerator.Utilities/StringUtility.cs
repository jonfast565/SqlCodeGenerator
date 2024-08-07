using System.Text;

namespace SqlCodeGenerator.Utilities;

public static class StringUtility
{
    public static string ToCamelCase(this string snakeCase)
    {
        var parts = snakeCase.Split('_');
        return parts[0] + string.Join("", parts.Skip(1).Select(p => char.ToUpper(p[0]) + p.Substring(1)));
    }
    
    public static string SnakeToPascal(this string snakeCase)
    {
        if (string.IsNullOrEmpty(snakeCase))
        {
            return snakeCase;
        }

        var pascalCase = new StringBuilder();
        var capitalizeNext = true;

        foreach (var c in snakeCase)
        {
            if (c == '_')
            {
                capitalizeNext = true;
            }
            else
            {
                pascalCase.Append(capitalizeNext ? char.ToUpper(c) : c);
                capitalizeNext = false;
            }
        }

        return pascalCase.ToString();
    }

    public static string IndentLevel(int indent)
    {
        return Enumerable.Range(0, indent).Aggregate(string.Empty, (s, _) => s + "\t");
    }
    
    public static string WithIndentLevel(this string value, int indent)
    {
        var level = IndentLevel(indent);
        return $"{level}{value}";
    }
}