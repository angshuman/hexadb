using System;
using System.Collections.Generic;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Microsoft.Extensions.Logging;

namespace Hexastore.ScaleConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            // FlatModelTest.RunTest(appCount: 1, deviceCount: 50000, devicePropertyCount: 3, sendCount: 2, senderThreadCount: 10).Wait();

            Console.WriteLine("No Lock");
            Console.WriteLine("Rocks Optimizations");
            //Console.WriteLine("Batch Size 1");
            //Console.WriteLine("1 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 1, false, 1, true).Wait();

            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 1, true).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 1, true).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 1, true).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 1, true).Wait();

            //Console.WriteLine("Batch Size 10");
            //Console.WriteLine("1 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 1, false, 1, true).Wait();

            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 10, true).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 10, true).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 10, true).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 10, true).Wait();


            Console.WriteLine("Batch Size 50");
            //Console.WriteLine("1 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 1, false, 1, true).Wait();

            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 50, true).Wait();

            Console.WriteLine("32 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 50, true).Wait();

            Console.WriteLine("64 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 50, true).Wait();

            Console.WriteLine("128 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 50, true).Wait();

            Console.WriteLine("Batch Size 100");
            Console.WriteLine("1 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 1, false, 1, true).Wait();

            Console.WriteLine("10 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 100, true).Wait();

            Console.WriteLine("32 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 100, true).Wait();

            Console.WriteLine("64 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 100, true).Wait();

            Console.WriteLine("128 threads");
            FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 100, true).Wait();


            //Console.WriteLine("Default Rocks");
            //Console.WriteLine("Batch Size 1");
            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 1, false).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 1, false).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 1, false).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 1, false).Wait();

            //Console.WriteLine("Batch Size 10");
            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 10, false).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 10, false).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 10, false).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 10, false).Wait();


            //Console.WriteLine("Batch Size 50");
            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 50, false).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 50, false).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 50, false).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 50, false).Wait();

            //Console.WriteLine("Batch Size 100");
            //Console.WriteLine("10 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 10, false, 100, false).Wait();

            //Console.WriteLine("32 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 32, false, 100, false).Wait();

            //Console.WriteLine("64 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 64, false, 100, false).Wait();

            //Console.WriteLine("128 threads");
            //FlatModelTest.RunTest(appCount: 1, deviceCount: 1000000, devicePropertyCount: 3, sendCount: 5, senderThreadCount: 128, false, 100, false).Wait();

        }
    }
}
