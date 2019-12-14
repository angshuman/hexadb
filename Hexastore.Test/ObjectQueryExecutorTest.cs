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
    public class ObjectQueryExecutorTest : RocksFixture
    {
        public ObjectQueryExecutorTest()
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
        public void Query_Value_Id_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Id = "300",
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple = new Triple("300", "age", (new JValue(30), -1));

            CollectionAssert.Contains(values, testTriple);
            Assert.AreEqual(1, values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Value_Id_Returns_Empty()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Id = "210",
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);

            Assert.AreEqual(0, rsp.Values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Value_Single_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name500" }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple = new Triple("500", "name", (new JValue("name500"), -1));
            CollectionAssert.Contains(values, testTriple);
            Assert.AreEqual(1, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Value_Multiple_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name500" },
                    ["age"] = new QueryUnit { Operator = "eq", Value = 20 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple = new Triple("500", "name", (new JValue("name500"), -1));
            CollectionAssert.Contains(values, testTriple);
            Assert.AreEqual(1, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Value_GT_Multiple_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["age"] = new QueryUnit { Operator = "ge", Value = 25 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple1 = new Triple("300", "age", (new JValue(30), -1));
            var testTriple2 = new Triple("700", "age", (new JValue(30), -1));
            CollectionAssert.Contains(values, testTriple1);
            CollectionAssert.Contains(values, testTriple2);
            Assert.AreEqual(2, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Value_GE_Multiple_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                    ["age"] = new QueryUnit { Operator = "ge", Value = 30 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple1 = new Triple("300", "name", (new JValue("name300"), -1));
            var testTriple2 = new Triple("700", "name", (new JValue("name700"), -1));
            CollectionAssert.Contains(values, testTriple1);
            CollectionAssert.Contains(values, testTriple2);
            Assert.AreEqual(2, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Value_LE_Multiple_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                    ["age"] = new QueryUnit { Operator = "le", Value = 20 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            var values = rsp.Values.ToArray();
            var testTriple1 = new Triple("100", "name", (new JValue("name100"), -1));
            var testTriple2 = new Triple("500", "name", (new JValue("name500"), -1));
            CollectionAssert.Contains(values, testTriple1);
            CollectionAssert.Contains(values, testTriple2);
            Assert.AreEqual(2, rsp.Values.Count());
            Assert.AreEqual(null, rsp.Continuation);
        }

        [TestMethod]
        public void Query_Value_LE_Multiple_Returns_Empty()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                    ["age"] = new QueryUnit { Operator = "le", Value = 10 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            Assert.AreEqual(0, rsp.Values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Value_LE_Single_Returns_Empty()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["age"] = new QueryUnit { Operator = "le", Value = 10 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            Assert.AreEqual(0, rsp.Values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_Value_EQ_Single_Returns_Empty()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "notpresent" }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            Assert.AreEqual(0, rsp.Values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }


        [TestMethod]
        public void Query_Value_EQ_Multiple_Returns_Empty()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "eq", Value = "name100" },
                    ["age"] = new QueryUnit { Operator = "eq", Value = 12 }
                },
                PageSize = 10
            };

            var rsp = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            Assert.AreEqual(0, rsp.Values.Count());
            Assert.AreEqual(rsp.Continuation, null);
        }

        [TestMethod]
        public void Query_With_Continuation_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                },
                PageSize = 2
            };

            var rsp1 = new ObjectQueryExecutor().Query(query, (RocksGraph)set);
            Assert.AreEqual(2, rsp1.Values.Count());
            Assert.AreEqual(rsp1.Continuation, new Triple("200", "name", TripleObject.FromData("name200")));

            var query2 = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                },
                Continuation = rsp1.Continuation,
                PageSize = 2
            };

            var rsp2 = new ObjectQueryExecutor().Query(query2, (RocksGraph)set);
            Assert.AreEqual(2, rsp2.Values.Count());
            Assert.AreEqual(rsp2.Continuation, new Triple("400", "name", TripleObject.FromData("name400")));

            var query3 = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                },
                Continuation = rsp2.Continuation,
                PageSize = 2
            };

            var rsp3 = new ObjectQueryExecutor().Query(query3, (RocksGraph)set);
            Assert.AreEqual(2, rsp3.Values.Count());
            Assert.AreEqual(rsp3.Continuation, new Triple("600", "name", TripleObject.FromData("name600")));

            var query4 = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                },
                Continuation = rsp3.Continuation,
                PageSize = 2
            };

            var rsp4 = new ObjectQueryExecutor().Query(query4, (RocksGraph)set);
            Assert.AreEqual(2, rsp4.Values.Count());
            Assert.AreEqual(rsp4.Continuation, new Triple("800", "name", TripleObject.FromData("name800")));

            var query5 = new ObjectQueryModel
            {
                Filter = new Dictionary<string, QueryUnit>()
                {
                    ["name"] = new QueryUnit { Operator = "contains", Value = "name" },
                },
                Continuation = rsp4.Continuation,
                PageSize = 2
            };

            var rsp5 = new ObjectQueryExecutor().Query(query5, (RocksGraph)set);
            Assert.AreEqual(0, rsp5.Values.Count());
            Assert.AreEqual(rsp5.Continuation, null);
        }
    }
}
