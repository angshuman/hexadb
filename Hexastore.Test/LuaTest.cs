using System;
using System.Linq;
using Hexastore.Graph;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class LuaTest
    {
        [TestMethod]
        public void LuaTest_Compute()
        {
            var factory = new LoggerFactory();
            var logger = factory.CreateLogger<RocksGraphProvider>();
            var storeLogger = factory.CreateLogger<StoreProcessor>();

            using var testFolder = new TestFolder();
            using var provider = new RocksGraphProvider(logger, testFolder);

            var storeProvider = new SetProvider(provider);
            var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeLogger);

            // Add device
            var device1 = new JObject(new JProperty("id", "device1"),
                new JProperty("type", "device"),
                new JProperty("temp", 70),
                new JProperty("humidity", 50));

            storeProcessor.PatchJson("test", new JArray(device1));

            // Query
            var query = new JObject(
                new JProperty("filter", new JObject(
                    new JProperty("type", new JObject(
                        new JProperty("op", "eq"),
                        new JProperty("value", "device"))))));

            var result = storeProcessor.Query("test", query, Array.Empty<string>(), 10);
        }
    }
}
