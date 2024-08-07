using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;
using SqlCodeGenerator.Interfaces;
using SqlCodeGenerator.Models;
using SqlCodeGenerator.Utilities;

namespace SqlCodeGenerator.CSharpAdapter;

public class CSharpCodeGenerator(ILogger logger) : ICodeGenerator
{
    public string GenerateDatabaseCode(IDatabaseMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var generatedClasses = new StringBuilder();
        var generatedMethods = new StringBuilder();
        var generatedAccessoryMethods = new StringBuilder();

        var tables = metadata.GetTableMetadata();
        foreach (var table in tables)
        {
            logger.LogInformation("Generate code for table {Table}", table.TableName);
            if (table.PrimaryKey.Count > 0)
            {
                generatedClasses.AppendLine($"// Class for table {table.TableName}");
                generatedClasses.AppendLine(GenerateClass(table, weaver));
                generatedClasses.AppendLine();
                generatedMethods.AppendLine($"// Methods for table {table.TableName}");
                generatedMethods.AppendLine(GenerateInsertMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateUpsertMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateUpdateMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateDeleteMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectIdMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectIdRangeMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectIdListMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectSearchMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectPagedMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine(GenerateSelectPagedWindowMethod(table, dbCodeGenerator, weaver));
                generatedMethods.AppendLine();
            }
            else
            {
                logger.LogWarning("Table should always have a " +
                                  "primary key for query generation, code not generated for {TableName}", table.TableName);
            }
        }
        
        generatedAccessoryMethods.AppendLine();

        var stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(generatedClasses.ToString());
        stringBuilder.AppendLine(generatedMethods.ToString());
        stringBuilder.AppendLine(generatedAccessoryMethods.ToString());

        return stringBuilder.ToString();
    }

    private static string GenerateInsertMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateInsertQuery(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindingsFromClass(metadata);
        method.AppendLine(
            $"public async Task<{tableName}> Insert{tableName}({tableName} entity)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, true, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateUpsertMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateUpsertQuery(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindingsFromClass(metadata);
        method.AppendLine(
            $"public async Task<{tableName}> Upsert{tableName}({tableName} entity)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, true, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateUpdateMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateUpdateQuery(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindingsFromClass(metadata);
        method.AppendLine(
            $"public async Task<{tableName}> Update{tableName}({tableName} entity)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, true, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateDeleteMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateDeleteQuery(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindings(metadata.Columns);
        method.AppendLine(
            $"public async Task<List<{tableName}>> Delete{tableName}({GenerateMethodParameters(metadata.GetPrimaryKeyColumns())})");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectStatement(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        method.AppendLine($"public async Task<List<{tableName}>> Select{tableName}()");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectIdMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectIdStatement(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindings(metadata.GetPrimaryKeyColumns());
        method.AppendLine(
            $"public async Task<{tableName}> Select{tableName}ById({GenerateMethodParameters(metadata.GetPrimaryKeyColumns())})");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, true, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectIdRangeMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectIdRangeStatement(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = GenerateParameterBindings(metadata.GetPrimaryKeyColumns());
        method.AppendLine(
            $"public async Task<List<{tableName}>> Select{tableName}ByIdRange({GenerateMethodParameters(metadata.GetPrimaryKeyColumns())})");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectIdListMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectIdListStatement(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = metadata.Columns.Select(x =>
            $"command.Parameters.AddWithValue(\"@ids\", ids);").ToArray();
        method.AppendLine(
            $"public async Task<List<{tableName}>> Select{tableName}ByIdList({GenerateMethodParameters(metadata.GetPrimaryKeyColumns())})");
        method.AppendLine("{");
        
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectSearchMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectSearchStatement(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = metadata.Columns.Select(x =>
            $"command.Parameters.AddWithValue(\"@{x.ColumnName}\", $\"%{{searchTerm}}%\");").ToArray();
        method.AppendLine($"public async Task<List<{tableName}>> Search{tableName}(string searchTerm)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateSelectPagedMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectPagedStatementLimitOffset(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = new[]
        {
            $"command.Parameters.AddWithValue(\"@offset\", offset);",
            $"command.Parameters.AddWithValue(\"@limit\", limit);"
        };
        method.AppendLine(
            $"public async Task<List<{tableName}>> Select{tableName}Paged(int offset, int limit)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }
    
    private static string GenerateSelectPagedWindowMethod(TableMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var query = dbCodeGenerator.GenerateSelectPagedStatementWindowed(metadata);
        var tableName = weaver.GenerateTableClassName(metadata);
        var parameters = new[]
        {
            $"command.Parameters.AddWithValue(\"@offset\", offset);",
            $"command.Parameters.AddWithValue(\"@limit\", limit);"
        };
        method.AppendLine(
            $"public async Task<List<{tableName}>> Select{tableName}Paged(int offset, int limit)");
        method.AppendLine("{");
        method.AppendLine(GenerateConnectionReader(metadata, weaver, query, false, parameters));
        method.AppendLine("}");
        return method.ToString();
    }

    private static string GenerateConnectionReader(
        TableMetadata metadata, 
        ICodeGenerationWeaver weaver, 
        string query, 
        bool singleResult = false,
        IEnumerable<string>? parameters = null)
    {
        var method = new StringBuilder();
        method.AppendLine($"{StringUtility.IndentLevel(1)}string query = @\"{query}\";");
        method.AppendLine($"{StringUtility.IndentLevel(1)}try");
        method.AppendLine($"{StringUtility.IndentLevel(1)}{{");
        method.AppendLine($"{StringUtility.IndentLevel(1)}{weaver.GenerateConnectionStatement(metadata.Engine)}");
        method.AppendLine($"{StringUtility.IndentLevel(1)}{weaver.GenerateCommandStatement(metadata.Engine)}");
        if (parameters != null)
        {
            foreach (var parameter in parameters)
            {
                method.AppendLine($"{StringUtility.IndentLevel(1)}{parameter}");
            }
        }
        method.AppendLine($"{StringUtility.IndentLevel(2)}using var reader = await command.ExecuteReaderAsync();");
        method.AppendLine($"{StringUtility.IndentLevel(2)}{GenerateReaderLoop("results", metadata, weaver)}");
        method.AppendLine($"{StringUtility.IndentLevel(2)}return results{(singleResult ? ".FirstOrDefault()" : string.Empty)};");
        method.AppendLine($"{StringUtility.IndentLevel(1)}}}");
        method.AppendLine($"{StringUtility.IndentLevel(1)}catch ({weaver.GenerateCustomExceptionType()} e)");
        method.AppendLine($"{StringUtility.IndentLevel(1)}{{");
        method.AppendLine($"{StringUtility.IndentLevel(1)}throw;");
        method.AppendLine($"{StringUtility.IndentLevel(1)}}}");
        return method.ToString();
    }
    
    private static string GenerateMethodParameters(List<(string ColumnName, string DataType)> columns)
    {
        return string.Join(", ", columns.Select(c => 
            $"{GetCSharpType(c.DataType)} {c.ColumnName.ToCamelCase()}"));
    }

    private static IEnumerable<string> GenerateParameterBindings(List<(string ColumnName, string DataType)> columns)
    {
        return columns.Select(column => 
            $"command.Parameters.AddWithValue(\"@{column.ColumnName}\", {column.ColumnName.ToCamelCase()});")
            .ToList();
    }
    
    private static IEnumerable<string> GenerateParameterBindingsFromClass(TableMetadata metadata)
    {
        return metadata.Columns.Select(column => 
                $"command.Parameters.AddWithValue(\"@{column.ColumnName}\", " +
                $"entity.{column.ColumnName.SnakeToPascal()});")
            .ToList();
    }

    private static string GetCSharpType(string sqlType)
    {
        return sqlType.ToLower() switch
        {
            "integer" => "int",
            "smallint" => "short",
            "bigint" => "long",
            "serial" => "int",
            "bigserial" => "long",
            "decimal" => "decimal",
            "numeric" => "decimal",
            "real" => "float",
            "double precision" => "double",
            "boolean" => "bool",
            "text" => "string",
            "varchar" => "string",
            "char" => "string",
            "date" => "DateTime",
            "timestamp" => "DateTime",
            "timestamp without time zone" => "DateTime",
            "timestamp with time zone" => "DateTimeOffset",
            _ => "string"
        };
    }

    private static string GenerateReaderLoop(string readerLoopVariable, TableMetadata metadata, ICodeGenerationWeaver weaver)
    {
        var method = new StringBuilder();
        var tableName = weaver.GenerateTableClassName(metadata);
        method.AppendLine($"{StringUtility.IndentLevel(2)}var {readerLoopVariable} = new List<{tableName}>();");
        method.AppendLine($"{StringUtility.IndentLevel(2)}while (await reader.ReadAsync())");
        method.AppendLine($"{StringUtility.IndentLevel(2)}{{");
        method.AppendLine($"{StringUtility.IndentLevel(3)}var result = new {tableName}();");

        foreach (var (columnName, dataType) in metadata.Columns)
        {
            var propertyName = weaver.GenerateColumnName(metadata.Engine, columnName);
            var csharpType = GetCSharpType(dataType);

            // method.AppendLine($"{StringUtility.IndentLevel(2)}if (!reader.IsDBNull(reader.GetOrdinal(\"{columnName}\")))");
            // method.AppendLine($"{StringUtility.IndentLevel(2)}{{");
            method.AppendLine($"{StringUtility.IndentLevel(3)}result.{propertyName} = reader.GetFieldValue<{csharpType}?>(\"{columnName}\", null);");
            // method.AppendLine($"{StringUtility.IndentLevel(2)}}}");
        }

        method.AppendLine($"{StringUtility.IndentLevel(3)}{readerLoopVariable}.Add(result);");
        method.AppendLine($"{StringUtility.IndentLevel(2)}}}");

        return method.ToString();
    }
    
    private static string GenerateClass(TableMetadata metadata, ICodeGenerationWeaver weaver)
    {
        var classCode = new StringBuilder();
        classCode.AppendLine($"public class {weaver.GenerateTableClassName(metadata)}");
        classCode.AppendLine("{");
        foreach (var column in metadata.Columns)
        {
            classCode.AppendLine(
                $"{StringUtility.IndentLevel(1)}" +
                $"public {GetCSharpType(column.DataType)}? {weaver.GenerateColumnName(metadata.Engine, column.ColumnName)} " +
                "{{ get; set; }}");
        }

        classCode.AppendLine("}");
        return classCode.ToString();
    }
}