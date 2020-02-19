using System;
using System.Collections.Concurrent;
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
        private readonly ConcurrentDictionary<string, RocksDb> _dbs = new ConcurrentDictionary<string, RocksDb>();
        private readonly ILogger _logger;
        private readonly string _rootPath = null;
        private readonly DbOptions _dbOptions = new DbOptions().SetCreateIfMissing(true);


        public RocksGraphProvider(ILogger<RocksGraphProvider> logger, string path = null, DbOptions optionInput = null)
        {
            _logger = logger;
            _rootPath = path;

            if (_rootPath == null) {
                var configPath = Environment.GetEnvironmentVariable("HEXASTORE_DATA_PATH");
                if (!string.IsNullOrEmpty(configPath)) {
                    _rootPath = Path.GetFullPath(configPath);
                }
            }

            if (_rootPath == null) {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) {
                    _rootPath = "/var/data/hexastore";
                } else {
                    _rootPath = "C:\\data\\hexastore";
                }
            }

            Directory.CreateDirectory(_rootPath);

            if (optionInput != null) {
                _dbOptions = optionInput;
            } else {
                _dbOptions.SetAllowConcurrentMemtableWrite(true);
            }

            /*
            .SetAllowConcurrentMemtableWrite(true)
            .SetAllowMmapReads(true)
            .SetAllowMmapWrites(true)
            .SetUseFsync(0)
            .IncreaseParallelism(6)
            .SetCompression(Compression.No);
            */
        }


        public bool ContainsGraph(string id, GraphType type)
        {
            var key = MakeKey(id, type);
            var db = GetDBOrNull(id);
            return db?.Get(key) == "1";
        }

        public IStoreGraph CreateGraph(string id, GraphType type)
        {
            var key = MakeKey(id, type);
            var db = GetOrCreateDB(id);
            db.Put(key, "1");
            return new RocksGraph(key, db);
        }

        public bool DeleteGraph(string id, GraphType type)
        {
            var db = GetDBOrNull(id);

            if (db == null || !ContainsGraph(id, type)) {
                return false;
            }
            db.Remove(MakeKey(id, type));
            return true;
        }

        public void WriteKey(string id, string key, string value)
        {
            GetDB(id).Put(key, value, null, _writeOptions);
        }

        public string ReadKey(string id, string key)
        {
            return GetDB(id).Get(key);
        }

        public void Dispose()
        {
            _logger.LogInformation("disposing rocksdb");

            foreach (var pair in _dbs) {
                pair.Value.Dispose();
            }

            _dbs.Clear();
        }

        public IStoreGraph GetGraph(string id, GraphType type)
        {
            var db = GetDB(id);
            string key = MakeKey(id, type);
            return new RocksGraph(key, db);
        }

        private static string MakeKey(string id, GraphType type)
        {
            return $"{(int)type}"; ;
        }

        private RocksDb GetDB(string id)
        {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentException("message", nameof(id));
            }

            var db = GetDBOrNull(id);

            if (db == null) {
                throw new FileNotFoundException($"Unable to find rocks db: {id}");
            }

            return db;
        }

        private RocksDb GetDBOrNull(string id)
        {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentException("message", nameof(id));
            }

            // Get the db if it exists, otherwise null is returned
            _dbs.TryGetValue(id, out var db);

            return db;
        }

        private RocksDb GetOrCreateDB(string id)
        {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentException("message", nameof(id));
            }

            var db = GetDBOrNull(id);

            if (db == null) {
                // Allow only one db to be created at a time to avoid conflicts
                lock (_dbs) {
                    // Double check lock
                    if (!_dbs.TryGetValue(id, out db)) {
                        var dbPath = Path.Combine(_rootPath, id);

                        db = RocksDb.Open(_dbOptions, dbPath);
                        _logger.LogInformation($"created rocksdb at {dbPath}");

                        if (!_dbs.TryAdd(id, db)) {
                            // this should never happen
                            throw new Exception("unable to add db");
                        }
                    }
                }
            }

            return db;
        }
    }
}
