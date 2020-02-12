using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.TestCommon;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using RocksDbSharp;

namespace Hexastore.ScaleConsole
{
    public static class FlatModelTest
    {
        private static readonly Random _random = new Random(234234);

        public static async Task RunTest(int appCount, int deviceCount, int devicePropertyCount, int sendCount, int senderThreadCount, bool useLock, int batchSize, bool tryOptimizeRocks)
        {
            var apps = new List<string>(appCount);
            var deviceIds = new List<string>(deviceCount);
            var devicePropertyNames = new List<string>(devicePropertyCount);
            var tasks = new List<Task>();
            var sendQueues = new List<ConcurrentQueue<TestMessage>>();
            //var sendQueue = new ConcurrentQueue<TestMessage>();
            for (var i = 0; i < senderThreadCount; i++)
            {
                sendQueues.Add(new ConcurrentQueue<TestMessage>());
            }

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
                var factory = new LoggerFactory();
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
                    .IncreaseParallelism(32)
                    .SetMaxBackgroundCompactions(32)
                    .SetMaxBackgroundFlushes(32));
                var storeProvider = new SetProvider(provider);
                var storeOperationFactory = new StoreOperationFactory();
                var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeOperationFactory, storeLogger, useLock);
                var count = 0;
                for (var i = 0; i < sendCount; i++)
                {
                    foreach (var id in deviceIds)
                    {
                        foreach (var app in apps)
                        {
                            var bucketIndex = HashString(id) % senderThreadCount;
                            var sendQueue = sendQueues[bucketIndex];
                            var message = new TestMessage()
                            {
                                App = app,
                                Device = id,
                                PropertyNames = devicePropertyNames
                            };
                            var points = GetPropertyValues(message.PropertyNames, _random);
                            message.Json = JsonGenerator.GenerateTelemetry(message.Device, points);
                            sendQueue.Enqueue(message);
                            count++;
                        }
                    }
                }

                Console.WriteLine($"Starting send of {count} messages");

                var timer = Stopwatch.StartNew();

                for (var i = 0; i < senderThreadCount; i++)
                {
                    var sendQueue = sendQueues[i];
                    tasks.Add(Task.Run(async () => await RunSender(storeProcessor, sendQueue, batchSize)));
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"Completed in {timer.Elapsed}");
            }
        }

        private static int HashString(string text)
        {
            // TODO: Determine nullity policy.

            unchecked
            {
                int hash = 23;
                foreach (char c in text)
                {
                    hash = hash * 31 + c;
                }
                return Math.Abs(hash);
            }
        }

        private static async Task RunSender(StoreProcessor storeProcessor, ConcurrentQueue<TestMessage> sendQueue, int batchSize)
        {
            var storeDict = new Dictionary<string, JArray>();
            while (sendQueue.TryDequeue(out var message))
            {
                if (!storeDict.ContainsKey(message.App))
                {
                    storeDict[message.App] = new JArray();
                }
                storeDict[message.App].Add(message.Json);
                if(storeDict[message.App].Count >= batchSize)
                {
                    await SendBatchEvent(storeProcessor, message.App, storeDict[message.App]);
                    storeDict[message.App].Clear();
                }
            }
            foreach(var key in storeDict.Keys)
            {
                await SendBatchEvent(storeProcessor, key, storeDict[key]);
                storeDict[key].Clear();
            }
        }

        private static async Task SendBatchEvent(StoreProcessor storeProcessor, string AppId, JArray messages)
        {
            //var points = GetPropertyValues(message.PropertyNames, _random);
            //var json = JsonGenerator.GenerateTelemetry(message.Device, points);
            await storeProcessor.PatchJson2(AppId, messages);
        }

        private static async Task SendEvent(StoreProcessor storeProcessor, TestMessage message)
        {
            //var points = GetPropertyValues(message.PropertyNames, _random);
            //var json = JsonGenerator.GenerateTelemetry(message.Device, points);
            await storeProcessor.PatchJson2(message.App, message.Json);
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
