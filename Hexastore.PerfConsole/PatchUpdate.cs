using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Hexastore.PerfConsole
{
    /**
     * Create a single database for all invocations.
     * 
     * Update multiple ids changing the telemetry data points on each patch.
     */
    [SimpleJob(RunStrategy.Monitoring, invocationCount: 2, launchCount: 2, warmupCount: 2, targetCount: 2)]
    public class PatchUpdate
    {
        private TestFolder _testFolder;
        private StoreProcessor _storeProcessor;
        private List<string> _ids = new List<string>();
        private List<string> _dataPoints = new List<string>();
        private const int _maxIds = 1000;
        private const int _maxPoints = 1000;
        private JArray jsonObjects = new JArray();


        [GlobalSetup]
        public void Setup()
        {
            _testFolder = new TestFolder();

            while (_ids.Count < _maxIds) {
                _ids.Add(Guid.NewGuid().ToString());
            }

            var pointCount = 0;
            while (_dataPoints.Count < _maxPoints) {
                _dataPoints.Add($"prop{pointCount++:D3}");
            }

            var factory = new LoggerFactory();
            var logger = factory.CreateLogger<RocksGraphProvider>();
            var storeLogger = factory.CreateLogger<StoreProcessor>();
            var provider = new RocksGraphProvider(logger, _testFolder);
            var storeProvider = new SetProvider(provider);
            _storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeLogger);

            var points = new Dictionary<string, double>();

            int x = 0;
            foreach (var id in _ids) {
                x++;

                foreach (var key in _dataPoints) {
                    points[key] = x;
                }

                var json = JsonGenerator.GenerateTelemetry(id, points);
                jsonObjects.Add(json);
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _testFolder.Dispose();
        }

        [Benchmark]
        public void RunTest()
        {
            var storeId = $"test{Guid.NewGuid()}";
            _storeProcessor.AssertBatch(storeId, jsonObjects, false);
        }
    }
}
