using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Rocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hexastore.Test
{
    [TestClass]
    public class ContinuationTest : RocksFixture
    {
        private readonly RocksGraph _set;

        public ContinuationTest()
        {
            var (set, _, _) = StoreProcessor.GetGraphs(SetId);
            _set = (RocksGraph)set;
            foreach (var s in Enumerable.Range(0, 10)) {
                foreach (var p in Enumerable.Range(0, 10)) {
                    foreach (var o in Enumerable.Range(0, 10)) {
                        _set.Assert(s.ToString(), p.ToString(), (o.ToString(), o));
                    }
                }
            }
        }

        [TestMethod]
        public void SP_Continuation_Returns()
        {
            var spc = _set.SP("5", "5", new Triple("5", "5", ("1", 1)));
            var next = spc.First();
            Assert.AreEqual(next, new Triple("5", "5", ("2", 2)));

            var sc = _set.S("5", new Triple("5", "2", ("3", 3)));
            Assert.AreEqual(new Triple("5", "2", ("4", 4)), sc.First());

            Assert.AreEqual(44, _set.S("5", new Triple("5", "5", ("5", 5))).ToArray().Count());
            Assert.AreEqual(4, _set.SP("5", "5", new Triple("5", "5", ("5", 5))).ToArray().Count());

            Assert.ThrowsException<InvalidOperationException>(() => _set.S("5", new Triple("4", "9", ("9", 9))));

            Assert.AreEqual(0, _set.S("5", new Triple("6", "5", ("5", 5))).ToArray().Count());
            Assert.AreEqual(0, _set.SP("5", "6", new Triple("5", "7", ("5", 5))).ToArray().Count());
        }

        [TestMethod]
        public void PO_Continuation_Returns()
        {
            var poc = _set.PO("4", "4", new Triple("4", "4", ("4", 4)));
            var pocFirst = poc.First();
            Assert.AreEqual(new Triple("5", "4", ("4", 4)), pocFirst);

            var pc = _set.P("4", new Triple("3", "4", ("1", 1)));
            Assert.AreEqual(new Triple("4", "4", ("1", 1)), pc.First());

            Assert.AreEqual(44, _set.P("5", new Triple("5", "5", ("5", 5))).ToArray().Count());
            Assert.AreEqual(4, _set.PO("5", "5", new Triple("5", "5", ("5", 5))).ToArray().Count());

            Assert.ThrowsException<InvalidOperationException>(() => _set.O("5", new Triple("4", "3", ("1", 1))));
            Assert.ThrowsException<InvalidOperationException>(() => _set.OS("5", "2", new Triple("1", "3", ("5", 5))));

            Assert.AreEqual(0, _set.P("5", new Triple("5", "6", ("5", 5))).ToArray().Count());
            Assert.AreEqual(0, _set.PO("6", "4", new Triple("5", "6", ("5", 5))).ToArray().Count());
        }

        [TestMethod]
        public void OS_Continuation_Returns()
        {
            var osc = _set.OS("4", "4", new Triple("4", "4", ("4", 4)));
            Assert.AreEqual(new Triple("4", "5", ("4", 4)), osc.First());

            var oc = _set.O("4", new Triple("3", "4", ("4", 4)));
            Assert.AreEqual(new Triple("3", "5", ("4", 4)), oc.First());

            Assert.AreEqual(44, _set.O("5", new Triple("5", "5", ("5", 5))).ToArray().Count());
            Assert.AreEqual(4, _set.OS("5", "5", new Triple("5", "5", ("5", 5))).ToArray().Count());

            Assert.ThrowsException<InvalidOperationException>(() => _set.O("5", new Triple("4", "3", ("1", 1))));
            Assert.ThrowsException<InvalidOperationException>(() => _set.OS("1", "3", new Triple("2", "3", ("1", 1))));

            Assert.AreEqual(0, _set.O("5", new Triple("5", "5", ("6", 6))).ToArray().Count());
            Assert.AreEqual(0, _set.OS("6", "4", new Triple("5", "5", ("6", 6))).ToArray().Count());
        }
    }
}
