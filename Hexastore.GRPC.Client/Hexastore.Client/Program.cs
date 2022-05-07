using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Hexastore.GRPC;

namespace Hexastore.Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            int count = -1;

            var tasks = Enumerable.Range(0, 8).Select(x => {
                return Task.Run(async () => {

                    var rsp = Interlocked.Increment(ref count);
                    var writer = new HexWriter($"blah{count:D3}");
                    while (true) {
                        await writer.WriteAsync(1000, 10);
                    }
                });
            });
            await Task.WhenAll(tasks);
        }
    }

    public class HexWriter
    {
        private readonly Random _rand = new();
        private readonly Ingest.IngestClient _client = new Ingest.IngestClient(GrpcChannel.ForAddress("https://localhost:5001"));
        private readonly string _storeId;
        private readonly Stopwatch _watch = Stopwatch.StartNew();
        private readonly Timer _timer;

        public long Total { get; set; }

        public HexWriter(string storeId)
        {
            _storeId = storeId;
            _timer = new Timer(Callback, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        private void Callback(object state)
        {
            Console.WriteLine($"Store {_storeId}: Total {Total} in {_watch.ElapsedMilliseconds}ms. Rate {(Total * 1000 / _watch.ElapsedMilliseconds)}/sec");
        }

        public async Task WriteAsync(int batchSize, int numProps)
        {
            var triples = new List<TripleMessage>();

            for (int i = 0; i < batchSize; i++) {
                triples.Add(GetTriple($"{i}", "type", "twin"));

                for (int j = 0; j < numProps; j++) {
                    triples.Add(GetTriple($"{i}", $"prop{j:D3}", _rand.Next(0, 100)));
                }
            };

            var request = new AssertRequest {
                StoreId = _storeId,
            };

            request.Triples.AddRange(triples);

            var response = await _client.AssertAsync(request);
            Total += batchSize;

        }

        public TripleMessage GetTriple(string s, string p, object o, bool isId = false, int arrayIndex = -1)
        {
            var t = new TripleMessage {
                Subject = s,
                Predicate = p
            };

            if (isId) {
                t.Object = o.ToString();
                return t;
            };


            switch (o) {
                case int oi:
                    t.IntValue = oi;
                    t.Type = TripleMessage.Types.ValueType.Int;
                    break;
                case double od:
                    t.DoubleValue = od;
                    t.Type = TripleMessage.Types.ValueType.Double;
                    break;
                case string os:
                    t.StringValue = os;
                    t.Type = TripleMessage.Types.ValueType.String;
                    break;
                case bool ob:
                    t.BoolValue = ob;
                    t.Type = TripleMessage.Types.ValueType.Bool;
                    break;
                default:
                    throw new InvalidOperationException("Unknown type");
            }

            t.ArrayIndex = arrayIndex;

            return t;
        }
    }
}
