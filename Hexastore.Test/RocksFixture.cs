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
            const string testDirectory = "./testdata";
            if (Directory.Exists(testDirectory)) {
                Directory.Delete(testDirectory, true);
            }
            Directory.CreateDirectory(testDirectory);
            TestFolder = new TestFolder();
            GraphProvider = new RocksGraphProvider(Mock.Of<ILogger<RocksGraphProvider>>(), TestFolder.Root);
            StoreProvider = new SetProvider(GraphProvider);
            StoreOperationFactory = new StoreOperationFactory();
            StoreProcessor = new StoreProcessor(StoreProvider, new Reasoner(), StoreOperationFactory, Mock.Of<ILogger<StoreProcessor>>());
            SetId = "testset";
        }

        public void Dispose()
        {
            GraphProvider.Dispose();
            TestFolder.Dispose();
        }
    }
}
