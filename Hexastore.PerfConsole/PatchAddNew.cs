using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Microsoft.Extensions.Logging;

namespace Hexastore.PerfConsole
{
    /**
     * Add only new telemetry to the db using Patch
     * 
     * A new database will be created for each invocation.
     */
    [SimpleJob(RunStrategy.Monitoring, invocationCount: 20)]
    public class PatchAddNew
    {
        private List<string> _ids = new List<string>();
        private List<string> _dataPoints = new List<string>();
        private const int _maxIds = 1000;
        private const int _maxPoints = 20;


        [GlobalSetup]
        public void Setup()
        {
            while (_ids.Count < _maxIds)
            {
                _ids.Add(Guid.NewGuid().ToString());
            }

            while (_dataPoints.Count < _maxPoints)
            {
                _dataPoints.Add(Guid.NewGuid().ToString());
            }
        }

        [Benchmark]
        public void RunTest()
        {
            using (var testFolder = new TestFolder())
            {
                var factory = new LoggerFactory();
                var logger = factory.CreateLogger<RocksGraphProvider>();
                var storeLogger = factory.CreateLogger<StoreProcessor>();
                var provider = new RocksGraphProvider(logger, testFolder);
                var storeProvider = new SetProvider(provider);
                var storeOperationFactory = new StoreOperationFactory();
                var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeOperationFactory, storeLogger);


                var storeId = "test";
                var points = new Dictionary<string, double>();
                var pointCount = 0;
                foreach (var pointId in _dataPoints)
                {
                    pointCount++;
                    points.Add(pointId, pointCount + 0.234);
                }

                foreach (var id in _ids)
                {
                    var json = JsonGenerator.GenerateTelemetry(id, points);
                    storeProcessor.PatchJson(storeId, json);
                }
            }
        }
    }
}
