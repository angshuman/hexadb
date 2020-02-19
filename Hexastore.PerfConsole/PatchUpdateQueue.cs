using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Hexastore.Errors;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Hexastore.Web;
using Hexastore.Web.EventHubs;
using Hexastore.Web.Queue;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Hexastore.PerfConsole
{
    [SimpleJob(RunStrategy.Monitoring, invocationCount: 20)]
    public class PatchUpdateQueue
    {
        private TestFolder _testFolder;
        private StoreProcessor _storeProcessor;
        private QueueContainer _queueContainer;
        private EventSender _eventSender;
        private List<string> _ids = new List<string>();
        private List<string> _dataPoints = new List<string>();
        private const int _updateCount = 20;
        private const int _maxIds = 10;
        private const int _maxPoints = 3;

        [GlobalSetup]
        public void Setup()
        {
            _testFolder = new TestFolder();

            while (_ids.Count < _maxIds) {
                _ids.Add(Guid.NewGuid().ToString());
            }

            while (_dataPoints.Count < _maxPoints) {
                _dataPoints.Add(Guid.NewGuid().ToString());
            }

            var factory = new LoggerFactory();
            var logger = factory.CreateLogger<RocksGraphProvider>();
            var storeLogger = factory.CreateLogger<StoreProcessor>();
            var graphProvider = new RocksGraphProvider(logger, _testFolder);
            var storeProvider = new SetProvider(graphProvider);
            _storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeLogger);
            var checkpoint = new Checkpoint(graphProvider);
            var storeConfig = new StoreConfig();
            var eventReceiver = new EventReceiver(_storeProcessor, checkpoint, storeConfig, factory.CreateLogger<EventReceiver>());
            _queueContainer = new QueueContainer(eventReceiver, factory.CreateLogger<QueueContainer>(), new StoreError());
            _eventSender = new EventSender(_queueContainer, eventReceiver, checkpoint, storeConfig);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _testFolder.Dispose();
        }

        [Benchmark]
        public async Task RunTest()
        {
            var storeId = "test";
            var points = new Dictionary<string, double>();
            var pointCount = 0;
            foreach (var pointId in _dataPoints) {
                pointCount++;
                points.Add(pointId, pointCount + 0.234);
            }

            for (var i = 0; i < _updateCount; i++) {
                foreach (var id in _ids) {
                    var json = JsonGenerator.GenerateTelemetry(id, points);
                    await _eventSender.SendMessage(
                        new StoreEvent {
                            Data = json,
                            StoreId = storeId,
                            PartitionId = id,
                            Operation = "PATCH_JSON",
                        });
                }
            }

            while(_queueContainer.Count() != 0) {
                await Task.Delay(10);
            }
        }
    }
}
