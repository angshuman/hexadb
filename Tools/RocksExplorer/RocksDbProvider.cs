namespace RocksExplorer
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using RocksDbSharp;

    public class RocksDbProvider
    {
        private readonly string _path;

        public RocksDbProvider(string path)
        {
            _path = path;
        }

        public async Task Read(int skip, int take, string outfile)
        {
            if (string.IsNullOrEmpty(outfile)) {
                Console.WriteLine("No output file provided");
                return;
            }

            var options = new DbOptions().SetCreateIfMissing(false);

            using var fs = new FileStream(outfile, FileMode.Create);
            using var streamWriter = new StreamWriter(fs);
            using var db = RocksDb.Open(options, _path);
            using var iterator = db.NewIterator();

            var watch = Stopwatch.StartNew();

            var first = iterator.SeekToFirst();
            var total = 0;
            for (int i = 0; i < skip + take; i++) {
                if (iterator.Valid()) {
                    total++;
                    if (i >= skip) {
                        await streamWriter.WriteAsync($"{i} | ");
                        await PrintString(streamWriter, iterator.Key());
                        await streamWriter.WriteAsync(" | ");
                        await PrintString(streamWriter, iterator.Value());
                        await streamWriter.WriteLineAsync();
                    }

                    iterator.Next();
                } else {
                    break;
                }
            }

            watch.Stop();
            Console.WriteLine($"Skip:{skip} Take:{take} Total:{total} Time:{watch.Elapsed.TotalSeconds}  Keys Read - {take * 1000.0 / watch.ElapsedMilliseconds} / sec");
        }

        public void Write(int count)
        {
            var options = new DbOptions().SetCreateIfMissing(true);
            var writeOptions = new RocksDbSharp.WriteOptions().SetSync(false);

            var watch = Stopwatch.StartNew();

            using var db = RocksDb.Open(options, _path);
            for (var i = 0; i < count; i++) {
                db.Put(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), null, writeOptions, Encoding.UTF8);
            }

            watch.Stop();
            Console.WriteLine($"Keys Written Count: {count} Time {watch.Elapsed.TotalSeconds} Rate: {count * 1000.0 / watch.ElapsedMilliseconds} / sec");
        }

        private async Task PrintString(StreamWriter streamWriter, byte[] bytes)
        {
            foreach (var b in bytes) {
                if ((short)b > 31 && (short)b < 127) {
                    await streamWriter.WriteAsync((char)b);
                } else {
                    await streamWriter.WriteAsync($"[{(short)b}]");
                }
            }
        }
    }
}
