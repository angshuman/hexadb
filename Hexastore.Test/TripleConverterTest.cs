using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Parser;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class TripleConverterTest
    {
        public IGraph _set;

        public TripleConverterTest()
        {
        }

        [TestMethod]
        public void To_Triples()
        {
            var item = new
            {
                id = "100",
                name = "name100",
                age = 20,
                contains = new
                {
                    name = "name200",
                    values = new object[] {
                        new
                        {
                            name = "name300",
                            age = 30,
                        },
                        new
                        {
                            name = "name400"
                        }
                    }
                }
            };

            var json = JObject.FromObject(item);
            var graph = TripleConverter.FromJson(json);

            var expected = new MemoryGraph();
            expected.Assert("100", "name", TripleObject.FromData("name100"));
            expected.Assert("100", "age", TripleObject.FromData(20));
            expected.Assert("100", "contains", "100#contains");
            expected.Assert("100", "name", TripleObject.FromData("name100"));
            expected.Assert("100#contains", "name", TripleObject.FromData("name200"));
            expected.Assert("100#contains", "values", "100#contains#values#0");
            expected.Assert("100#contains", "values", "100#contains#values#1");
            expected.Assert("100#contains#values#0", "name", TripleObject.FromData("name300"));
            expected.Assert("100#contains#values#0", "age", TripleObject.FromData(30));
            expected.Assert("100#contains#values#1", "name", TripleObject.FromData("name400"));
            CollectionAssert.AreEquivalent(expected.GetTriples().ToArray(), graph.ToArray());
        }

        [TestMethod]
        public void To_Patch()
        {
            var item = new
            {
                id = "100",
                name = "name101",
                contains = new
                {
                    name = "name200",
                    values = new object[] {
                        new
                        {
                            age = (int?)null,
                        }
                    }
                }
            };

            var json = JObject.FromObject(item);
            var graph = TripleConverter.FromJson(json);

            var expected = new List<Triple>();
            expected.Add(new Triple("100", "name", TripleObject.FromData("name101")));
            expected.Add(new Triple("100", "contains", "100#contains"));
            expected.Add(new Triple("100#contains", "name", TripleObject.FromData("name200")));
            expected.Add(new Triple("100#contains", "values", "100#contains#values#0"));
            expected.Add(new Triple("100#contains#values#0", "age", TripleObject.FromData(null)));
            CollectionAssert.AreEquivalent(expected.ToArray(), graph.ToArray());
        }
    }
}
