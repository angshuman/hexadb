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
        public readonly TestFolder TestFolder;

        public RocksFixture()
        {
            TestFolder = new TestFolder();
            GraphProvider = new RocksGraphProvider(Mock.Of<ILogger<RocksGraphProvider>>(), TestFolder.Root);
            StoreProvider = new SetProvider(GraphProvider);
            StoreProcessor = new StoreProcessor(StoreProvider, new Reasoner(), Mock.Of<ILogger<StoreProcessor>>());
            SetId = "testset";
        }

        public void Dispose()
        {
            GraphProvider.Dispose();
            TestFolder.Dispose();
        }
    }
}
