using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Processor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class ArrayTest : RocksFixture
    {
        private object _item1;

        public ArrayTest()
        {
            _item1 = new
            {
                id = "simplearray",
                values = new int[] { 10, 20, 10, 11, 29, 12 }
            };

            StoreProcessor.Assert("app1", JToken.FromObject(_item1), false);
        }

        [TestMethod]
        public void Json_Read_Returns_Array()
        {
            var rsp = StoreProcessor.GetSubject("app1", "simplearray", null, 1);
            Assert.IsTrue(JToken.DeepEquals(JToken.FromObject(_item1), rsp));
        }
    }
}
