namespace SqlCodeGenerator.Models;

public class TableMetadata(
    DatabaseEngineType engine,
    string tableName,
    List<(string ColumnName, string DataType)> columns,
    List<string> primaryKey)
{
    public DatabaseEngineType Engine { get; set; }
    public string TableName { get; set; } = tableName;
    public List<(string ColumnName, string DataType)> Columns { get; set; } = columns;
    public List<string> PrimaryKey { get; set; } = primaryKey;

    public List<(string ColumnName, string DataType)> GetPrimaryKeyColumns()
    {
        return Columns.Where(c => PrimaryKey.Contains(c.ColumnName)).ToList();
    }
}