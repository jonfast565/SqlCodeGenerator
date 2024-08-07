namespace SqlCodeGenerator.Interfaces;

public interface ICodeGenerator
{
    string GenerateDatabaseCode(IDatabaseMetadata metadata, IDatabaseCodeGenerator dbCodeGenerator, ICodeGenerationWeaver weaver);
}