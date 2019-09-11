using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Processor;
using Hexastore.Query;
using Hexastore.Rocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class ObjectQueryOutgoingTests : RocksFixture
    {
        public ObjectQueryOutgoingTests()
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
        }

        [TestMethod]
        public void Query_With_Outgoing_EQ_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name100" }
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
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name300" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "100");
            Assert.AreEqual(1, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_With_Outgoing_Two_Levels_GT_Returns()
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
                                    ["age"] = new QueryUnit { Operator = "gt", Value = 25 }
                                }
                            }
                     }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "100");
            CollectionAssert.Contains(values, "500");
            CollectionAssert.Contains(values, "800");
            Assert.AreEqual(3, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_With_Outgoing_Single_Level_LT_Returns()
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
                        Path = "values",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["age"] = new QueryUnit { Operator = "lt", Value = 31 }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "200");
            CollectionAssert.Contains(values, "600");
            Assert.AreEqual(2, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_With_Outgoing_Single_Level_EQ_Returns()
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
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name400" }
                            },
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "100");
            Assert.AreEqual(1, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_With_Outgoing_Id_Returns()
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
                            Id = "800"
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "500");
            CollectionAssert.Contains(values, "800");
            Assert.AreEqual(2, rsp.Values.Count());
        }

        [TestMethod]
        public void Query_With_Outgoing_Eq_Returns()
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
                                ["name"] = new QueryUnit { Operator = "eq", Value = "name800" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.Select(x => x.Subject).ToArray();
            CollectionAssert.Contains(values, "500");
            CollectionAssert.Contains(values, "800");
            Assert.AreEqual(2, rsp.Values.Count());
        }
    }
}
