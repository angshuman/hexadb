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
     * Create a single database for all invocations.
     * 
     * Update telemetry for only 1 id.
     */
    [SimpleJob(RunStrategy.Monitoring, invocationCount: 20)]
    public class PatchUpdateSingle
    {
        private TestFolder _testFolder;
        private StoreProcessor _storeProcessor;
        private List<string> _ids = new List<string>();
        private List<string> _dataPoints = new List<string>();
        private const int _updateCount = 100;
        private const int _maxIds = 1;
        private const int _maxPoints = 10;


        [GlobalSetup]
        public void Setup()
        {
            _testFolder = new TestFolder();
            
            while (_ids.Count < _maxIds)
            {
                _ids.Add(Guid.NewGuid().ToString());
            }

            while (_dataPoints.Count < _maxPoints)
            {
                _dataPoints.Add(Guid.NewGuid().ToString());
            }

            var factory = new LoggerFactory();
            var logger = factory.CreateLogger<RocksGraphProvider>();
            var storeLogger = factory.CreateLogger<StoreProcessor>();
            var provider = new RocksGraphProvider(logger, _testFolder);
            var storeProvider = new SetProvider(provider);
            _storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeLogger);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _testFolder.Dispose();
        }

        [Benchmark]
        public void RunTest()
        {
            var storeId = "test";
            var points = new Dictionary<string, double>();
            var pointCount = 0;
            foreach (var pointId in _dataPoints)
            {
                pointCount++;
                points.Add(pointId, pointCount + 0.234);
            }
            int x = 0;

            for (var i = 0; i < _updateCount; i++)
            {
                foreach (var id in _ids)
                {
                    x++;

                    foreach (var key in _dataPoints)
                    {
                        points[key] += x;
                    }

                    var json = JsonGenerator.GenerateTelemetry(id, points);
                    _storeProcessor.PatchJson(storeId, json);
                }
            }
        }
    }
}
