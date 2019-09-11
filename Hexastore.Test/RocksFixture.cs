using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.Store;

namespace Hexastore.Test
{
    public class RocksFixture : IDisposable
    {
        public readonly RocksGraphProvider GraphProvider;
        public readonly IStoreProvider StoreProvider;
        public readonly IStoreProcesor StoreProcessor;
        public readonly string SetId;

        public RocksFixture()
        {
            const string testDirectory = "./testdata";
            if (Directory.Exists(testDirectory)) {
                Directory.Delete(testDirectory, true);
            }
            Directory.CreateDirectory(testDirectory);
            GraphProvider = new RocksGraphProvider(testDirectory);
            StoreProvider = new SetProvider(GraphProvider);
            StoreProcessor = new StoreProcessor(StoreProvider, new Reasoner());
            SetId = "testset";
        }

        public void Dispose()
        {
            GraphProvider.Dispose();
        }
    }
}
