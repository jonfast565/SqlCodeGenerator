using SqlCodeGenerator.Models;

namespace SqlCodeGenerator.Interfaces;

public interface IDatabaseCodeGenerator
{
    string GenerateInsertQuery(TableMetadata table);
    string GenerateUpsertQuery(TableMetadata table);
    string GenerateUpdateQuery(TableMetadata table);
    string GenerateDeleteQuery(TableMetadata table);
    string GenerateSelectStatement(TableMetadata table);
    string GenerateSelectIdStatement(TableMetadata table);
    string GenerateSelectSearchStatement(TableMetadata table);
    string GenerateSelectIdRangeStatement(TableMetadata table);
    string GenerateSelectIdListStatement(TableMetadata table);
    string GenerateSelectPagedStatementLimitOffset(TableMetadata table);
    string GenerateSelectPagedStatementWindowed(TableMetadata table);
}