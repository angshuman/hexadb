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

namespace Hexastore.ScaleConsole
{
    public static class FlatModelTest
    {
        private static readonly Random _random = new Random(234234);

        public static void RunTest(int appCount, int deviceCount, int devicePropertyCount, int sendCount, int senderThreadCount)
        {
            var apps = new List<string>(appCount);
            var deviceIds = new List<string>(deviceCount);
            var devicePropertyNames = new List<string>(devicePropertyCount);
            var tasks = new List<Task>();
            var sendQueue = new ConcurrentQueue<TestMessage>();

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
                var provider = new RocksGraphProvider(logger, testFolder);
                var storeProvider = new SetProvider(provider);
                var storeOperationFactory = new StoreOperationFactory();
                var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeOperationFactory, storeLogger);

                for (var i = 0; i < sendCount; i++)
                {
                    foreach (var id in deviceIds)
                    {
                        foreach (var app in apps)
                        {
                            sendQueue.Enqueue(new TestMessage()
                            {
                                App = app,
                                Device = id,
                                PropertyNames = devicePropertyNames
                            });
                        }
                    }
                }

                Console.WriteLine($"Starting send of {sendQueue.Count} messages");

                var timer = Stopwatch.StartNew();

                for (var i = 0; i < senderThreadCount; i++)
                {
                    tasks.Add(Task.Run(() => RunSender(storeProcessor, sendQueue)));
                }

                Task.WhenAll(tasks).Wait();

                Console.WriteLine($"Completed in {timer.Elapsed}");
            }
        }

        private static void RunSender(StoreProcessor storeProcessor, ConcurrentQueue<TestMessage> sendQueue)
        {
            while (sendQueue.TryDequeue(out var message))
            {
                SendEvent(storeProcessor, message);
            }
        }

        private static void SendEvent(StoreProcessor storeProcessor, TestMessage message)
        {
            var points = GetPropertyValues(message.PropertyNames, _random);
            var json = JsonGenerator.GenerateTelemetry(message.Device, points);
            storeProcessor.PatchJson(message.App, json);
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
