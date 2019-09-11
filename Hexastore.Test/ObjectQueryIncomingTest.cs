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
    public class ObjectQueryIncomingTest : RocksFixture
    {
        public ObjectQueryIncomingTest()
        {
            dynamic item1 = new
            {
                id = "100",
                name = "name100",
                age = 20,
                contains = new
                {
                    id = "200",
                    name = "name200",
                    values = new dynamic[] {
                        new
                        {
                            id = "300",
                            name = "name300",
                            age = 30,
                        },
                        new
                        {
                            id = "400",
                            name = "name400"
                        }
                    }
                }
            };

            dynamic item2 = new
            {
                id = "500",
                name = "name500",
                age = 20,
                contains = new
                {
                    id = "600",
                    name = "name600",
                    values = new dynamic[] {
                        new
                        {
                            id = "700",
                            name = "name700",
                            age = 30,
                        },
                        new
                        {
                            id = "800",
                            name = "name800",
                            contains = new
                            {
                                id = "600"
                            }
                        }
                    }
                }
            };

            StoreProcessor.Assert("app1", JToken.FromObject(item1), false);
            StoreProcessor.Assert("app1", JToken.FromObject(item2), false);

            for (int i = 0; i < 100; i++) {
                var item = new
                {
                    id = "root001",
                    name = "root1",
                    contains = new
                    {
                        id = "c1",
                        name = "namec1",
                        values = new
                        {
                            id = $"i{i.ToString("D2")}",
                            name = $"name{i.ToString("D2")}",
                        }
                    }
                };
                StoreProcessor.Assert("app2", JToken.FromObject(item), false);
            }

            for (int i = 0; i < 100; i++) {
                var item = new
                {
                    id = "root002",
                    name = "root2",
                    contains = new
                    {
                        id = "c2",
                        name = "namec2",
                        values = new
                        {
                            id = $"j{i.ToString("D2")}",
                            name = $"name{i.ToString("D2")}",
                        }
                    }
                };
                StoreProcessor.Assert("app2", JToken.FromObject(item), false);
            }
        }

        [TestMethod]
        public void Query_With_Incoming_Double_Level_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name300" }
                },
                PageSize = 10,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name100" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "300");
            Assert.AreEqual(1, values.Length);
        }

        [TestMethod]
        public void Query_With_Incoming_Single_Level_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name300" }
                },
                PageSize = 10,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name200" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "300");
            Assert.AreEqual(1, values.Length);
        }

        [TestMethod]
        public void Query_With_Incoming_Paged_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app2");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" }
                },
                PageSize = 10,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "root1" }
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
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "root1" }
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
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "contains#values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "root1" }
                            }
                        }
                    }
                }
            };

            var rsp3 = new ObjectQueryExecutor().Query(query3, (RocksGraph)set);
            var values3 = rsp3.Values.Select(x => x.Subject).ToArray();
            Assert.AreEqual(null, rsp3.Continuation);
            Assert.AreEqual(80, values3.Length);
        }
    }
}
