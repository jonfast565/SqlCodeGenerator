using Microsoft.Extensions.Logging;
using Npgsql;
using SqlCodeGenerator;
using SqlCodeGenerator.Interfaces;
using SqlCodeGenerator.Models;

namespace PostgresAdapter;

public class PostgresMetadata(NpgsqlConnection connection, ILogger logger) : IDatabaseMetadata
{
    private List<string> GetTables()
    {
        logger.LogInformation("Get tables from {Database}", connection.Database);
        var tables = new List<string>();
        var query = $"""
                     SELECT table_name
                     FROM information_schema.tables
                     WHERE table_schema = 'public'
                     AND table_type = 'BASE TABLE';
                     """;
        using var command = new NpgsqlCommand(query, connection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    private List<string> GetPrimaryKeyColumns(string tableName)
    {
        logger.LogInformation("Get primary key columns for table {TableName}", tableName);
        var primaryKeyColumns = new List<string>();
        var query = $"""
                     SELECT a.attname
                     FROM   pg_index i
                     JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                           AND a.attnum = ANY(i.indkey)
                     WHERE  i.indrelid = '{tableName}'::regclass
                     AND    i.indisprimary;
                     """;

        using var command = new NpgsqlCommand(query, connection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            primaryKeyColumns.Add(reader.GetString(0));
        }

        return primaryKeyColumns;
    }

    private List<(string ColumnName, string DataType)> GetColumnsWithTypes(string tableName)
    {
        logger.LogInformation("Get columns for table {TableName}", tableName);
        var columns = new List<(string ColumnName, string DataType)>();
        var query = $"""
                     SELECT column_name, data_type 
                     FROM information_schema.columns 
                     WHERE table_name = '{tableName}';
                     """;

        using var command = new NpgsqlCommand(query, connection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            columns.Add((reader.GetString(0), reader.GetString(1)));
        }

        return columns;
    }

    public List<TableMetadata> GetTableMetadata()
    {
        var tables = GetTables();
        return (from table in tables 
            let primaryKey = GetPrimaryKeyColumns(table) 
            let columns = GetColumnsWithTypes(table) 
            select new TableMetadata(DatabaseEngineType.Postgres, table, columns, primaryKey)).ToList();
    }

    public List<List<(string ColumnName, string DataType)>> GetQueryMetadata(string query)
    {
        var metadata = new List<List<(string ColumnName, string ColumnType)>>();

        using var cmd = new NpgsqlCommand(query, connection);
        using var reader = cmd.ExecuteReader();
        
        do
        {
            var resultSetMetadata = new List<(string ColumnName, string DataType)>();

            for (var i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var columnType = reader.GetDataTypeName(i);
                resultSetMetadata.Add((columnName, columnType));
            }

            metadata.Add(resultSetMetadata);
        }
        while (reader.NextResult());

        return metadata;
    }
}