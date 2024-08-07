using SqlCodeGenerator.Models;

namespace SqlCodeGenerator.Interfaces;

public interface ICodeGenerationWeaver
{
    public string GenerateTableClassName(TableMetadata metadata);
    public string GenerateConnectionStatement(DatabaseEngineType type);
    string GenerateColumnName(DatabaseEngineType type, string originalColumnName);
    string GenerateCommandStatement(DatabaseEngineType type);
    string GenerateCustomExceptionType();
}