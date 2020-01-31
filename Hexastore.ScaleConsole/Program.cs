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
            FlatModelTest.RunTest(appCount: 1, deviceCount: 300000, devicePropertyCount: 3, sendCount: 10, senderThreadCount: 10);
        }
    }
}
