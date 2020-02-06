using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Hexastore.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class ObjectQueryExecutorPathTest : RocksFixture
    {
        public ObjectQueryExecutorPathTest()
        {
            var text = File.ReadAllText("./Data/json1.json");
            StoreProcessor.Assert("app1", JToken.FromObject(text), false);
        }

        [TestMethod]
        public void Query_With_Star_Path_At_Beginning_Returns()
        {
            var (set, _, _) = StoreProcessor.GetGraphs("app1");

            var query = new ObjectQueryModel {
                Filter = new Dictionary<string, QueryUnit>() {
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
    }
}
