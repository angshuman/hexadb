using BenchmarkDotNet.Running;

namespace Hexastore.PerfConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            // These can be executed with:
            // dotnet run -c Release
            // Results will be in Hexastore.PerfConsole\BenchmarkDotNet.Artifacts\results
            BenchmarkRunner.Run<PatchAddNew>();
            BenchmarkRunner.Run<PatchUpdate>();
            BenchmarkRunner.Run<PatchUpdateSingle>();
            BenchmarkRunner.Run<PatchUpdateNoChange>();
        }
    }
}
