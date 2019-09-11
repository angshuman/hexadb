using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Query;
using Hexastore.Rocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class ObjectQueryExecutorPagedTest : RocksFixture
    {
        public ObjectQueryExecutorPagedTest()
        {
            for (int i = 0; i < 100; i++) {
                var item = new
                {
                    id = $"i{i.ToString("D2")}",
                    name = $"name{i.ToString("D2")}",
                    param = i,
                    contains = new
                    {
                        id = "c0",
                        name = "namec0",
                        values = new
                        {
                            id = "v0",
                            name = "namev0"
                        }
                    }
                };
                StoreProcessor.Assert("app1", JToken.FromObject(item), false);
            }

            for (int j = 0; j < 100; j++) {
                var item = new
                {
                    id = $"j{j.ToString("D2")}",
                    name = $"name{j.ToString("D2")}",
                    param = j,
                    contains = new
                    {
                        id = "c1",
                        name = "namec1",
                        values = new
                        {
                            id = "v1",
                            name = "namev1"
                        }
                    }
                };
                StoreProcessor.Assert("app1", JToken.FromObject(item), false);
            }
        }

        [TestMethod]
        public void Query_With_PageSize_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" }
                },
                PageSize = 10,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "namev0" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.AreEqual(values, new string[] { "i00", "i01", "i02", "i03", "i04", "i05", "i06", "i07", "i08", "i09" });
            Assert.AreEqual(new Triple("i09", "name", TripleObject.FromData("name09")), rsp.Continuation);

            var query2 = new ObjectQueryModel
            {
                Continuation = rsp.Continuation,
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" }
                },
                PageSize = 10,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "namev0" }
                            }
                        }
                    }
                }
            };

            var rsp2 = new ObjectQueryExecutor().Query(query2, (RocksGraph)set);
            var values2 = rsp2.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.AreEqual(values2, new string[] { "i10", "i11", "i12", "i13", "i14", "i15", "i16", "i17", "i18", "i19" });
            Assert.AreEqual(new Triple("i19", "name", TripleObject.FromData("name19")), rsp2.Continuation);

            var query3 = new ObjectQueryModel
            {
                Continuation = rsp2.Continuation,
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" }
                },
                PageSize = 1000,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "namev0" }
                            }
                        }
                    }
                }
            };

            var rsp3 = new ObjectQueryExecutor().Query(query3, (RocksGraph)set);
            var values3 = rsp3.Values.Select(x => x.Subject).ToArray();
            Assert.AreEqual(80, values3.Length);
            Assert.AreEqual(null, rsp3.Continuation);
        }
    }
}
