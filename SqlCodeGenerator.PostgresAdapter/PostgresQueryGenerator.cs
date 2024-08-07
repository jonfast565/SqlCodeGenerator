using SqlCodeGenerator;
using SqlCodeGenerator.Interfaces;
using SqlCodeGenerator.Models;

namespace PostgresAdapter;

public class PostgresQueryGenerator : IDatabaseCodeGenerator
{
    public string GenerateInsertQuery(TableMetadata table)
    {
        var query = $"""
                     INSERT INTO {table.TableName} ({string.Join(", ", table.Columns.Select(c => c.ColumnName))}) 
                     VALUES ({string.Join(", ", table.Columns.Select(c => "@" + c.ColumnName))}) 
                     RETURNING {string.Join(", ", table.Columns.Select(c => c.ColumnName))};
                     """;
        return query;
    }

    public string GenerateUpsertQuery(TableMetadata table)
    {
        var query = $"""
                     INSERT INTO {table.TableName} ({string.Join(", ", table.Columns.Select(c => c.ColumnName))}) 
                     VALUES ({string.Join(", ", table.Columns.Select(c => "@" + c.ColumnName))}) 
                     ON CONFLICT ({string.Join(", ", table.PrimaryKey)}) 
                     DO UPDATE SET {string.Join(", ", table.Columns.Where(c => !table.PrimaryKey.Contains(c.ColumnName)).Select(c => "\n\t\t\t\t\t" + c.ColumnName + " = EXCLUDED." + c.ColumnName))} 
                     RETURNING {string.Join(", ", table.Columns.Select(c => c.ColumnName))};
                     """;
        return query;
    }

    public string GenerateUpdateQuery(TableMetadata table)
    {
        var query = $"""
                     UPDATE {table.TableName} 
                     SET {string.Join(", ", table.Columns.Where(c => !table.PrimaryKey.Contains(c.ColumnName)).Select(c => "\n\t\t\t\t\t" + c.ColumnName + " = @" + c.ColumnName))} 
                     WHERE {string.Join(" AND ", table.PrimaryKey.Select(pk => pk + " = @" + pk))} 
                     RETURNING {string.Join(", ", table.Columns.Select(c => c.ColumnName))};
                     """;
        return query;
    }

    public string GenerateDeleteQuery(TableMetadata table)
    {
        var query = $"""
                     DELETE FROM {table.TableName} 
                     WHERE {string.Join("\n\t\t\t\t\tAND ", table.PrimaryKey.Select(pk => pk + " = @" + pk))} 
                     RETURNING {string.Join(", ", table.Columns.Select(c => c.ColumnName))};
                     """;
        return query;
    }

    public string GenerateSelectStatement(TableMetadata table)
    {
        var query = $"""
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                     FROM {table.TableName};
                     """;
        return query;
    }

    public string GenerateSelectIdStatement(TableMetadata table)
    {
        var query = $"""
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                     FROM {table.TableName} 
                     WHERE {string.Join(" AND ", table.PrimaryKey.Select(pk => pk + " = @" + pk))};
                     """;
        return query;
    }

    public string GenerateSelectSearchStatement(TableMetadata table)
    {
        var query = @$"SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                   FROM {table.TableName} 
                   WHERE {string.Join(" OR ", table.Columns.Select(c => c.ColumnName + " ILIKE @" + c.ColumnName))};";
        return query;
    }

    public string GenerateSelectIdRangeStatement(TableMetadata table)
    {
        var query = $"""
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                     FROM {table.TableName} 
                     WHERE {string.Join(" AND ", table.PrimaryKey.Select(pk => pk + " BETWEEN @start_" + pk + " AND @end_" + pk))};
                     """;
        return query;
    }

    public string GenerateSelectIdListStatement(TableMetadata table)
    {
        var query = $"""
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                     FROM {table.TableName} 
                     WHERE {string.Join(" AND ", table.PrimaryKey.Select(pk => pk + " = ANY(@ids)"))};
                     """;
        return query;
    }

    public string GenerateSelectPagedStatementLimitOffset(TableMetadata table)
    {
        var query = $"""
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))} 
                     FROM {table.TableName} 
                     OFFSET @offset 
                     LIMIT @limit;
                     """;
        return query;
    }

    public string GenerateSelectPagedStatementWindowed(TableMetadata table)
    {
        var query = $"""
                     WITH paginated_query_{table.TableName} AS (
                         SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))},
                         ROW_NUMBER() OVER (ORDER BY {string.Join(", ", table.PrimaryKey)}) AS row_number
                         FROM {table.TableName} 
                     )
                     SELECT {string.Join(", ", table.Columns.Select(c => c.ColumnName))}
                     FROM paginated_query_{table.TableName}
                     WHERE row_number BETWEEN @offset AND @offset + @limit
                     """;
        return query;
    }
}