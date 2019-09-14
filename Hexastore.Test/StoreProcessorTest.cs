using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Parser;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class StoreProcessorTest : RocksFixture
    {
        private object[] _items;

        public StoreProcessorTest()
        {
            _items = new object[10];
            foreach (var i in Enumerable.Range(0, 10)) {
                dynamic item = new
                {
                    id = i.ToString(),
                    name = $"name{i}",
                    age = i * 5
                };
                _items[i] = item;
            }

            StoreProcessor.Assert("app1", JToken.FromObject(_items), false);

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

            StoreProcessor.Assert("app2", JToken.FromObject(item1), false);
            StoreProcessor.Assert("app2", JToken.FromObject(item2), false);
        }


        [TestMethod]
        public void Json_Read_Returns()
        {
            var rsp = StoreProcessor.GetSubject("app1", "1", null, 1);
            Assert.IsTrue(JToken.DeepEquals(JToken.FromObject(_items[1]), rsp));

            var rsp2 = StoreProcessor.GetSubject("app1", "2", null, 1);
            Assert.IsTrue(JToken.DeepEquals(JToken.FromObject(_items[2]), rsp2));
        }


        [TestMethod]
        public void Json_Read_Write_Relationship_Returns()
        {
            dynamic item = new
            {
                id = "100",
                name = "name100",
                contains = new
                {
                    id = "200",
                    name = "name200",
                    values = new dynamic[] {
                        new
                        {
                            id = "300",
                            name = "name300"
                        },
                        new
                        {
                            id = "400",
                            name = "name400"
                        }
                    }
                }
            };

            StoreProcessor.Assert("app1", JToken.FromObject(item), false);
            var rsp = StoreProcessor.GetSubject("app1", "100", null, 3);

            Assert.IsTrue(JToken.DeepEquals(JToken.FromObject(item), rsp));
        }

        [TestMethod]
        public void Patch_With_Relationship_Returns()
        {
            var patch = new
            {
                id = "100",
                name = (string)null,
                contains = new
                {
                    name = "name202",
                    values = new object[] {
                        new
                        {
                            name = "name303",
                            age = (int?)null,
                        }
                    }
                }
            };
            StoreProcessor.PatchJson("app2", JObject.FromObject(patch));

            var rsp = StoreProcessor.GetSubject("app2", "100", null, 3);
            var expected = new
            {
                id = "100",
                age = 20,
                contains = new
                {
                    id = "100#contains",
                    name = "name202",
                    values = new
                    {
                        id = "100#contains#values#0",
                        name = "name303"
                    }
                }
            };
            Assert.IsTrue(JObject.DeepEquals(JObject.FromObject(expected), rsp));
        }

        [TestMethod]
        public void Patch_With_Values_Returns()
        {
            var doc = new
            {
                id = "100",
                name = "Device 100",
                data = new
                {
                    temp = 50,
                    humidity = 70,
                    pressure = 1001.02,
                    region = "seattle/1"
                }
            };

            var patch = new
            {
                id = "100",
                data = new
                {
                    temp = 55,
                    pressure = 1001.03,
                    region = "seattle/1"
                }
            };

            var expected = new
            {
                id = "100",
                name = "Device 100",
                data = new
                {
                    id = "100#data",
                    temp = 55,
                    humidity = 70,
                    pressure = 1001.03,
                    region = "seattle/1",
                },
            };

            StoreProcessor.Assert("app3", JObject.FromObject(doc), false);
            StoreProcessor.PatchJson("app3", JObject.FromObject(patch));

            var rsp = StoreProcessor.GetSubject("app3", "100", null, 3);
            var rspString = TripleConverter.FromJson(rsp).ToArray();
            var expectedString = TripleConverter.FromJson(JObject.FromObject(expected)).ToArray();
            CollectionAssert.AreEquivalent(rspString, expectedString);
        }
    }
}
