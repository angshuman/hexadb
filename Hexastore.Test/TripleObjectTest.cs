using Hexastore.Graph;
using Hexastore.Rocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    [TestClass]
    public class TripleObjectTest
    {
        [TestMethod]
        public void Convert_To_Byte_Roundtrip_Returns()
        {
            var o1 = new TripleObject("abc", true, JTokenType.String, null);
            var bytes1 = o1.ToBytes();

            var roundtripped = bytes1.ToTripleObject();
            Assert.IsTrue(roundtripped.IsID);
            Assert.AreEqual(o1.Value, roundtripped.Value);
            Assert.AreEqual(o1.TokenType, roundtripped.TokenType);

            var o2 = TripleObject.FromData(5);
            var bytes2 = o2.ToBytes();

            var roundtripped2 = bytes2.ToTripleObject();
            Assert.IsFalse(roundtripped2.IsID);
            Assert.AreEqual(o2.Value, roundtripped2.Value);
            Assert.AreEqual(o2.TokenType, roundtripped2.TokenType);
        }
    }
}
