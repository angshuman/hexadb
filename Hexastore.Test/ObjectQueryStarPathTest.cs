using System;
using System.Collections.Generic;
using System.IO;
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
    public class ObjectQueryStarPathTest : RocksFixture
    {

        public ObjectQueryStarPathTest()
        {
            var text = File.ReadAllText(Path.Combine("TestInput", "region.json"));

            StoreProcessor.Assert("app1", JToken.Parse(text), true);
        }

        [TestMethod]
        public void Query_Outgoing_One_Level_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "building" }
                },
                PageSize = 10,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "*",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["type"] = new QueryUnit { Operator = "eq", Value = "floor" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            CollectionAssert.AreEquivalent(new string[] { "oBZ_JoNOBC", "zKbQyTeF" }, rsp.Values.Select(x => x.Subject).ToArray());
        }

        [TestMethod]
        public void Query_Outgoing_Two_Level_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "building" }
                },
                PageSize = 10,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "*/*",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "Entity room 4" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            Assert.AreEqual(1, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
            Assert.AreEqual("oBZ_JoNOBC", values.First().Subject);
        }

        [TestMethod]
        public void Query_Outgoing_Partial_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "building" }
                },
                PageSize = 10,
                HasObject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "*/rooms",
                        Target = new ObjectQueryModel
                        {
                            Filter = new Dictionary<string, QueryUnit>()
                            {
                                ["name"] = new QueryUnit { Operator = "eq", Value = "Entity room 4" }
                            }
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            Assert.AreEqual(1, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
            Assert.AreEqual("oBZ_JoNOBC", values.First().Subject);
        }

        [TestMethod]
        public void Query_Incoming_Three_Level_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "sensor" }
                },
                PageSize = 64,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "*/*/*",
                        Target = new ObjectQueryModel
                        {
                            Id = "oBZ_JoNOBC"
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            Assert.AreEqual(8, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Incoming_Partial_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "sensor" }
                },
                PageSize = 64,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "floors/*/sensors",
                        Target = new ObjectQueryModel
                        {
                            Id = "oBZ_JoNOBC"
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            Assert.AreEqual(8, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Incoming_Two_Level_Star_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
                    ["type"] = new QueryUnit { Operator = "eq", Value = "sensor" }
                },
                PageSize = 64,
                HasSubject = new LinkQuery[]
                {
                    new LinkQuery
                    {
                        Path = "*/*",
                        Target = new ObjectQueryModel
                        {
                            Id = "fR5pgeHPpH"
                        }
                    }
                }
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();

            Assert.AreEqual(4, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }
    }
}
