using SqlCodeGenerator.Models;

namespace SqlCodeGenerator.Interfaces;

public interface IDatabaseMetadata
{
    List<TableMetadata> GetTableMetadata();
    List<List<(string ColumnName, string DataType)>> GetQueryMetadata(string query);
}