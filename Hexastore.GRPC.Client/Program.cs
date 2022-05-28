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
            Greeter.GreeterClient client = new Greeter.GreeterClient(GrpcChannel.ForAddress("http://localhost:49185"));
            var response = await client.SayHelloAsync(new HelloRequest { Name = "HAL" });

            Console.WriteLine(response.Message);

#if false
            Console.WriteLine("Hello World!");
            int count = -1;

            var tasks = Enumerable.Range(0, 8).Select(x => {
                return Task.Run(async () => {

                    var rsp = Interlocked.Increment(ref count);
                    var writer = new HexDriver($"blah{count:D3}");
                    while (true) {
                        await writer.WriteAsync(1000, 10);
                    }
                });
            });
            await Task.WhenAll(tasks);
#endif

            var driver = new HexDriver("store1");
            // await driver.WriteList();
            await driver.WriteGraph();
            await driver.SelectAll();
            await driver.PointQueryAsync();
            await driver.UnrootedQueryAsync();
        }
    }

    public class HexDriver
    {
        private readonly Random _rand = new();
        private readonly Ingest.IngestClient _ingestClient = new Ingest.IngestClient(GrpcChannel.ForAddress("http://localhost:49185"));
        private readonly Query.QueryClient _queryClient = new Query.QueryClient(GrpcChannel.ForAddress("http://localhost:49185"));
        private readonly string _storeId;
        private readonly Stopwatch _watch = Stopwatch.StartNew();
        public long Total { get; set; }

        public HexDriver(string storeId)
        {
            _storeId = storeId;
        }

        private void Callback(object state)
        {
            Console.WriteLine($"Store {_storeId}: Total {Total} in {_watch.ElapsedMilliseconds}ms. Rate {(Total * 1000 / _watch.ElapsedMilliseconds)}/sec");
        }

        public async Task PointQueryAsync(string id = "1")
        {
            Console.WriteLine("Point Query");
            var watch = Stopwatch.StartNew();

            var query = new QueryRequest {
                StoreId = _storeId,
                Query = new ObjectQuery {
                    Id = id
                },
                Levels = 2
            };

            var response = await _queryClient.QueryAsync(query);
            Console.WriteLine(string.Join('\n', response.Triples.Select(x => x.Serialize())));
            Console.WriteLine($"Point Query In: {watch.ElapsedMilliseconds}");
        }

        public async Task RootedQueryAsync(string id = "1")
        {
            Console.WriteLine("################################################################## Rooted Query");
            var watch = Stopwatch.StartNew();

            var query = new QueryRequest {
                StoreId = _storeId,
                Query = new ObjectQuery {
                    PageSize = 1000,
                },
            };

            query.Query.Filter["prop005"] = new QueryUnit { Operator = "ge", IntValue = 50 };
            

            var linkQuery = new LinkQuery {
                Level = 5,
                Path = "*",
            };

            linkQuery.Target = new ObjectQuery{};
            linkQuery.Target.Filter["partitionId"] = new QueryUnit { Operator = "eq", StringValue = id };

            // linkQuery.Target = new ObjectQuery { Id = id };
            query.Query.HasSubject.Add(linkQuery);

            var response = await _queryClient.QueryAsync(query);
            Console.WriteLine(string.Join('\n', response.Triples.Select(x => x.Serialize())));
            Console.WriteLine($"Rooted Query In: {watch.ElapsedMilliseconds}");
        }

        public async Task UnrootedQueryAsync()
        {
            Console.WriteLine("################################################################## Unrooted Query");
            var watch = Stopwatch.StartNew();

            var query = new QueryRequest {
                StoreId = _storeId,
                Query = new ObjectQuery {
                    PageSize = 1000,
                },
                Levels = 0,
            };

            query.Query.Filter["prop005"] = new QueryUnit { Operator = "ge", IntValue = 50 };

            var linkQuery = new LinkQuery {
                Level = 5,
                Path = "*",
            };

            linkQuery.Target = new ObjectQuery { };
            linkQuery.Target.Filter["prop005"] = new QueryUnit { Operator = "ge", IntValue = 50 };
            query.Query.HasSubject.Add(linkQuery);

            var response = await _queryClient.QueryAsync(query);
            Console.WriteLine(string.Join('\n', response.Triples.Select(x => x.Serialize())));
            Console.WriteLine($"Unrooted Query In: {watch.ElapsedMilliseconds}");

        }

        public async Task SelectAll()
        {
            Console.WriteLine("################################################################## Select All");
            var watch = Stopwatch.StartNew();

            var query = new QueryRequest {
                StoreId = _storeId,
            };

            var response = _queryClient.SelectAll(query);
            var cts = new CancellationTokenSource();
            
            while(await response.ResponseStream.MoveNext(cts.Token)) {
                Console.WriteLine(response.ResponseStream.Current.Serialize());
            }
            
            Console.WriteLine($"Unrooted Query In: {watch.ElapsedMilliseconds}");
        }

        public async Task WriteList(int numTwins = 100, int numProps = 10)
        {
            using var timer = new Timer(Callback, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

            var triples = new List<TripleMessage>();

            for (int i = 0; i < numTwins; i++) {
                triples.Add(GetTriple($"{i}", "type", "twin"));

                for (int j = 0; j < numProps; j++) {
                    triples.Add(GetTriple($"{i}", $"prop{j:D3}", _rand.Next(0, 100)));
                }
            };

            var request = new AssertRequest {
                StoreId = _storeId,
            };

            request.Triples.AddRange(triples);

            var response = await _ingestClient.AssertAsync(request);
            Total += numTwins;
        }

        public async Task WriteGraph()
        {
            var batchSize = 1000;
            var data = new DataGenerator(15, 2, "contains", 10, 10, false).Generate();
            var batches = data.Batch(batchSize);

            foreach (var batch in batches) {
                var triples = new List<TripleMessage>();
                foreach (var item in batch) {
                    var tms = ConvertToTriples(item);
                    triples.AddRange(tms);
                }
                var request = new AssertRequest {
                    StoreId = _storeId
                };

                request.Triples.AddRange(triples);
                await _ingestClient.AssertAsync(request);
                Total += batchSize;
            }
        }

        private IEnumerable<TripleMessage> ConvertToTriples(object item)
        {
            switch (item) {
                case Node node:
                    yield return GetTriple(node.Id, "label", node.Label);
                    yield return GetTriple(node.Id, "partitionId", node.PartitionId);
                    foreach (var prop in node.Properties) {
                        yield return GetTriple(node.Id, prop.Key, prop.Value);
                    }
                    break;
                case Edge edge:
                    yield return GetTriple(edge.FromId, edge.Label, edge.ToId, true);
                    break;
                default:
                    throw new ArgumentException("Unknown graph entity");
            }
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
