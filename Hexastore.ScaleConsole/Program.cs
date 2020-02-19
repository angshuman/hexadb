namespace Hexastore.ScaleConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, tryOptimizeRocks: true);
        }
    }
}
