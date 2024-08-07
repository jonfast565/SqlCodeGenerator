using SqlCodeGenerator.Interfaces;
using SqlCodeGenerator.Models;
using SqlCodeGenerator.Utilities;

namespace SqlCodeGenerator.CSharpAdapter;

public class CSharpPostgresCodeWeaver : ICodeGenerationWeaver
{
    public string GenerateTableClassName(TableMetadata metadata)
    {
        return metadata.Engine switch
        {
            DatabaseEngineType.Postgres => metadata.TableName.SnakeToPascal(),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public string GenerateConnectionStatement(DatabaseEngineType type)
    {
        return type switch
        {
            DatabaseEngineType.Postgres => "using var conn = await _dbConnectionFactory.CreateConnection();",
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public string GenerateCommandStatement(DatabaseEngineType type)
    {
        return type switch
        {
            DatabaseEngineType.Postgres => "using var command = new NpgsqlCommand(query, conn);",
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public string GenerateColumnName(DatabaseEngineType type, string originalColumnName)
    {
	    return type switch
	    {
			DatabaseEngineType.Postgres => originalColumnName.SnakeToPascal(),
			_ => throw new ArgumentOutOfRangeException()
	    };
    }

    public string GenerateCustomExceptionType()
    {
	    return "NpgsqlException";
    }
}