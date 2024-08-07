using Microsoft.Extensions.Logging;
using PostgresAdapter;
using SqlCodeGenerator.CSharpAdapter;
using SqlCodeGenerator.Interfaces;

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = loggerFactory.CreateLogger<Program>();

const string connectionString = "Host=localhost;Port=10002;Database=fire;Username=postgres;";
await using var connection = new Npgsql.NpgsqlConnection(connectionString);
await connection.OpenAsync();

IDatabaseMetadata pgMetadata = new PostgresMetadata(connection, logger);
IDatabaseCodeGenerator pgQueryGenerator = new PostgresQueryGenerator();
ICodeGenerationWeaver pgCodeWeaver = new CSharpPostgresCodeWeaver();
ICodeGenerator csharpCodeGenerator = new CSharpCodeGenerator(logger);

var results = csharpCodeGenerator.GenerateDatabaseCode(pgMetadata, pgQueryGenerator, pgCodeWeaver);

const string outputPath = @"C:\Users\jfast\Desktop\GeneratedMethods.cs";
File.WriteAllText(outputPath, results);

