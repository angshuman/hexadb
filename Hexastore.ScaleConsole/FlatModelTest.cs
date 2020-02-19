using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Hexastore.Errors;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Hexastore.Web;
using Hexastore.Web.EventHubs;
using Hexastore.Web.Queue;
using Microsoft.Extensions.Logging;
using RocksDbSharp;

namespace Hexastore.ScaleConsole
{
    public static class FlatModelTest
    {
        private static readonly Random _random = new Random(234234);

        public static async Task RunTest(int appCount, int deviceCount, int devicePropertyCount, int sendCount, int senderThreadCount, bool tryOptimizeRocks)
        {
            Console.WriteLine("Creating Messages");
            var apps = new List<string>(appCount);
            var deviceIds = new List<string>(deviceCount);
            var devicePropertyNames = new List<string>(devicePropertyCount);
            var tasks = new List<Task>();
            var sendQueue = new ConcurrentQueue<StoreEvent>();

            while (apps.Count < appCount)
            {
                apps.Add(Guid.NewGuid().ToString());
            }

            while (deviceIds.Count < deviceCount)
            {
                deviceIds.Add(Guid.NewGuid().ToString());
            }

            while (devicePropertyNames.Count < devicePropertyCount)
            {
                devicePropertyNames.Add(Guid.NewGuid().ToString());
            }

            using (var testFolder = new TestFolder())
            {
                var factory = LoggerFactory.Create(builder => builder.AddConsole());
                var logger = factory.CreateLogger<RocksGraphProvider>();
                var storeLogger = factory.CreateLogger<StoreProcessor>();
                var dbOptions = new DbOptions();
                var provider = !tryOptimizeRocks ? 
                    new RocksGraphProvider(logger, testFolder) :
                    new RocksGraphProvider(logger, testFolder, dbOptions.SetCreateIfMissing(true)
                    .SetAllowConcurrentMemtableWrite(true)
                    //.SetAllowMmapReads(true)
                    //.SetAllowMmapWrites(true)
                    //.SetUseFsync(0)
                    .IncreaseParallelism(Environment.ProcessorCount)
                    .SetMaxBackgroundCompactions(Environment.ProcessorCount)
                    .SetMaxBackgroundFlushes(Environment.ProcessorCount));
                var storeProvider = new SetProvider(provider);
                var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeLogger);
                var storeConfig = new StoreConfig();
                var storeError = new StoreError();
                var eventReceiver = new EventReceiver(storeProcessor, null, storeConfig, factory.CreateLogger<EventReceiver>());
                var queueContainer = new QueueContainer(eventReceiver, factory.CreateLogger<QueueContainer>(), storeError, 1_000_000);
                var eventSender = new EventSender(queueContainer, null, null, storeConfig);
                var count = 0;
                for (var i = 0; i < sendCount; i++)
                {
                    foreach (var id in deviceIds)
                    {
                        foreach (var app in apps)
                        {
                            var points = GetPropertyValues(devicePropertyNames, _random);
                            var e = new StoreEvent {
                                Operation = EventType.PATCH_JSON,
                                Data = JsonGenerator.GenerateTelemetry(id, points),
                                PartitionId = id,
                                StoreId = app
                            };
                            sendQueue.Enqueue(e);
                            count++;
                        }
                    }
                }

                Console.WriteLine($"Starting send of {count} messages");

                var timer = Stopwatch.StartNew();

                for (var i = 0; i < senderThreadCount; i++)
                {
                    tasks.Add(Task.Run(async () => await RunSender(eventSender, sendQueue)));
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"Completed writing to queues in {timer.Elapsed}");

                while (queueContainer.Count() > 0) {
                    Thread.Sleep(1000);
                }
                Console.WriteLine($"Completed writing to storage in {timer.Elapsed}");

            }
        }

        private static async Task RunSender(EventSender eventSender, ConcurrentQueue<StoreEvent> sendQueue)
        {
            // TODO: add batching back in
            while (sendQueue.TryDequeue(out var message))
            {
                await eventSender.SendMessage(message);
            }
        }

        private static Dictionary<string, double> GetPropertyValues(List<string> devicePropertyNames, Random random)
        {
            var points = new Dictionary<string, double>();
            foreach (var pointId in devicePropertyNames)
            {
                points.Add(pointId, random.NextDouble());
            }

            return points;
        }
    }
}
