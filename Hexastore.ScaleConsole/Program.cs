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
            FlatModelTest.RunTest(appCount: 5, deviceCount: 100, devicePropertyCount: 1000, sendCount: 10, senderThreadCount: 10);
        }
    }
}
