using System;
using System.IO;
using System.Runtime.InteropServices;
using Hexastore.Graph;
using Hexastore.Store;
using Microsoft.Extensions.Logging;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    public class RocksGraphProvider : IGraphProvider, IDisposable
    {
        private static readonly WriteOptions _writeOptions = (new WriteOptions()).SetSync(false);
        private readonly RocksDb _db;
        ILogger _logger;


        public RocksGraphProvider(ILogger<RocksGraphProvider> logger, string path = null, DbOptions optionInput = null)
        {
            _logger = logger;
            string dataPath;
            if (path == null) {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) {
                    dataPath = "/var/data/hexastore";
                } else {
                    dataPath = "C:\\data\\hexastore";
                }
            } else {
                dataPath = path;
            }
            Directory.CreateDirectory(dataPath);
            var options = optionInput ?? new DbOptions()
                .SetCreateIfMissing(true);
            /*
            .SetAllowConcurrentMemtableWrite(true)
            .SetAllowMmapReads(true)
            .SetAllowMmapWrites(true)
            .SetUseFsync(0)
            .IncreaseParallelism(6)
            .SetCompression(Compression.No);
            */

            _db = RocksDb.Open(options, dataPath);
            _logger.LogInformation($"created rocksdb at {dataPath}");
        }

        public bool ContainsGraph(string id, GraphType type)
        {
            var key = MakeKey(id, type);
            return _db.Get(key) == "1";
        }

        public IStoreGraph CreateGraph(string id, GraphType type)
        {
            var key = MakeKey(id, type);
            _db.Put(key, "1");
            return new RocksGraph(key, _db);
        }

        public bool DeleteGraph(string id, GraphType type)
        {
            if (!ContainsGraph(id, type)) {
                return false;
            }
            _db.Remove(MakeKey(id, type));
            return true;
        }

        public void WriteKey(string key, string value)
        {
            _db.Put(key, value, null, _writeOptions);
        }

        public string ReadKey(string key)
        {
            return _db.Get(key);
        }

        public void Dispose()
        {
            _logger.LogInformation("disposing rocksdb");
            _db.Dispose();
        }

        public IStoreGraph GetGraph(string id, GraphType type)
        {
            if (ContainsGraph(id, type)) {
                string key = MakeKey(id, type);
                return new RocksGraph(key, _db);
            }
            throw new FileNotFoundException();
        }

        private string MakeKey(string id, GraphType type)
        {
            return $"{(int)type}-{id}"; ;
        }
    }

}
