using Microsoft.Extensions.Logging;
using PostgresAdapter;
using SqlCodeGenerator.CSharpAdapter;
using SqlCodeGenerator.Interfaces;
using SqlCodeGenerator.Models;

namespace SqlCodeGenerator.Factories;

public static class CodeGeneratorFactory
{
    public static async Task<CodeGeneratorProxy> GetCodeGenerator(DatabaseEngineType engine, BackendLanguageType lang, string connectionString, ILogger logger)
    {
        IDatabaseMetadata metadata;
        IDatabaseCodeGenerator queryGenerator;
        ICodeGenerationWeaver codeWeaver;
        ICodeGenerator codeGenerator;

        if (engine == DatabaseEngineType.Postgres)
        {
            await using var connection = new Npgsql.NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            metadata = new PostgresMetadata(connection, logger);
            queryGenerator = new PostgresQueryGenerator();
        }
        else
        {
            throw new Exception("Database engine not supported for query generation");
        }

        if (lang == BackendLanguageType.CSharp) 
        {
            codeGenerator = new CSharpCodeGenerator(logger);
            if (engine == DatabaseEngineType.Postgres)
            {
                codeWeaver = new CSharpPostgresCodeWeaver();
            }
            else
            {
                throw new Exception("Could not find weaver for this database engine");
            }
        }
        else
        {
            throw new Exception("Language not supported for query generation");
        }
        
        return new CodeGeneratorProxy
        {
            DatabaseMetadata = metadata,
            QueryGenerator = queryGenerator,
            CodeWeaver = codeWeaver,
            CodeGenerator = codeGenerator
        };
    }
}