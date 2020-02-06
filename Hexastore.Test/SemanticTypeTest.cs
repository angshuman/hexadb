using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Parser;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class SemanticTypeTest
    {
        [TestMethod]
        public void SemanticType_BasicTest()
        {
            using (var testFolder = new TestFolder()) {
                var factory = new LoggerFactory();
                var logger = factory.CreateLogger<RocksGraphProvider>();
                var storeLogger = factory.CreateLogger<StoreProcessor>();
                var provider = new RocksGraphProvider(logger, testFolder);
                var storeProvider = new SetProvider(provider);
                var storeOperationFactory = new StoreOperationFactory();
                var storeProcessor = new StoreProcessor(storeProvider, new Reasoner(), storeOperationFactory, storeLogger);
                var storeId = "test";

                // Populate telemetry
                storeProcessor.PatchJson(storeId, DataGenerator.GenerateTelemetry("freezer-1", "Temperature", 1.0));

                storeProcessor.PatchJson(storeId, DataGenerator.GenerateTelemetry("fridge-1", "Temperature", 1.1));
                storeProcessor.PatchJson(storeId, DataGenerator.GenerateTelemetry("fridge-1", "FridgeTemperature", 5.0));

                storeProcessor.PatchJson(storeId, DataGenerator.GenerateTelemetry("room-1", "Temperature", 20.0));

                // Query for all temperatures for interface
                
            }
        }
    }
}
