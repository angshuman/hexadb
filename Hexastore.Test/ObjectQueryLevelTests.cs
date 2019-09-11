using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class ObjectQueryLevelTests : RocksFixture
    {
        public ObjectQueryLevelTests()
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

            for (int i = 0; i < 10; i++) {
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

            for (int i = 0; i < 10; i++) {
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
        public void Query_Outgoing_Level_Returns()
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
                        Path = "*",
                        Level = 3,
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name300" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "100");
            CollectionAssert.Contains(values, "200");
            CollectionAssert.Contains(values, "300");
            Assert.AreEqual(3, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Outgoing_Level_Circular_Returns()
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
                        Path = "*",
                        Level = 3,
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name800" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "500");
            CollectionAssert.Contains(values, "600");
            CollectionAssert.Contains(values, "800");
            Assert.AreEqual(3, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Outgoing_Sub_Level_Circular_Returns()
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
                        Path = "*",
                        Level = 2,
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name800" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "600");
            CollectionAssert.Contains(values, "800");
            Assert.AreEqual(2, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_Incoming_Level_Returns()
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
                        Path = "*",
                        Level = 3,
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

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.AreEqual(values, new string[] { "i00", "i01", "i02", "i03", "i04", "i05", "i06", "i07", "i08", "i09" });
            Assert.AreEqual(new Triple("i09", "name", TripleObject.FromData("name09")), rsp.Continuation);
        }

        [TestMethod]
        public void Query_Incoming_Sub_Level_Returns()
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
                        Path = "*",
                        Level = 2,
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

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.AreEqual(values, new string[] { "c1" });
            Assert.AreEqual(null, rsp.Continuation);
        }
    }
}
