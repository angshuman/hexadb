using RocksDbSharp;
using System;
using System.IO;

namespace Hexastore.Rocks
{
    public class RocksTest
    {
        public RocksTest()
        {
            var dataPath = Path.Combine(Path.GetFullPath("."), "rockstest");
            if (Directory.Exists(dataPath)) {
                Directory.Delete(dataPath, true);
            }
            Directory.CreateDirectory("/var/data/rockstest");

            var options = new DbOptions().SetCreateIfMissing(true);
            using (var db = RocksDb.Open(options, "/var/data/rockstest")) {
                // Using strings below, but can also use byte arrays for both keys and values
                // much care has been taken to minimize buffer copying
                db.Put("key", "value1");
                string value = db.Get("key");
                Console.WriteLine(value);
                db.Remove("key");
            }
        }
    }
}
