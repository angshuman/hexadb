using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.Store;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting.Logging;
using Moq;

namespace Hexastore.Test
{
    public class RocksFixture : IDisposable
    {
        public readonly RocksGraphProvider GraphProvider;
        public readonly IStoreProvider StoreProvider;
        public readonly IStoreProcesor StoreProcessor;
        public readonly IStoreOperationFactory StoreOperationFactory;
        public readonly string SetId;
        public readonly string TestDirectory;

        public RocksFixture()
        {
            TestDirectory = $"./{Guid.NewGuid()}";
            if (Directory.Exists(TestDirectory)) {
                Directory.Delete(TestDirectory, true);
            }
            Directory.CreateDirectory(TestDirectory);
            GraphProvider = new RocksGraphProvider(Mock.Of<ILogger<RocksGraphProvider>>(), TestDirectory);
            StoreProvider = new SetProvider(GraphProvider);
            StoreOperationFactory = new StoreOperationFactory();
            StoreProcessor = new StoreProcessor(StoreProvider, new Reasoner(), StoreOperationFactory, Mock.Of<ILogger<StoreProcessor>>());
            SetId = "testset";
        }

        public void Dispose()
        {
            GraphProvider.Dispose();
            if (Directory.Exists(TestDirectory))
            {
                Directory.Delete(TestDirectory, true);
            }
        }
    }
}
