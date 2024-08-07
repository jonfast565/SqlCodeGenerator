using SqlCodeGenerator.Interfaces;

namespace SqlCodeGenerator.Factories;

public class CodeGeneratorProxy
{
    public IDatabaseMetadata DatabaseMetadata { get; set; }
    public IDatabaseCodeGenerator QueryGenerator { get; set; }
    public ICodeGenerationWeaver CodeWeaver { get; set; }
    public ICodeGenerator CodeGenerator { get; set; }
}