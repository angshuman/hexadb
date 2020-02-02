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

            FlatModelTest.RunTest(appCount: 1, deviceCount: 1, devicePropertyCount: 1, sendCount: 5, senderThreadCount: 1).Wait();
        }
    }
}
