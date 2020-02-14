using System.Linq;
using Hexastore.Graph;
using Hexastore.Processor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hexastore.Test
{
    [TestClass]
    public class TripleTest : RocksFixture
    {
        private readonly IGraph _set;

        public TripleTest()
        {
            var (set, _, _) = StoreProcessor.GetGraphs(SetId);
            _set = set;
        }

        [TestMethod]
        public void Assert_Exists_Returns()
        {
            var rsp1 = _set.Assert("a", "b", "c");
            Assert.AreEqual(rsp1, true);
            var rsp2 = _set.Assert("a", "b", TripleObject.FromData("c"));
            Assert.AreEqual(rsp2, true);

            var exist1 = _set.Exists("a", "b", "c");
            Assert.AreEqual(exist1, true);
            var exist2 = _set.Exists("a", "b", TripleObject.FromId("c"));
            Assert.AreEqual(exist2, true);
            var exist3 = _set.Exists("a", "b", "d");
            Assert.AreEqual(exist3, false);
            var exist4 = _set.Exists("b", "b", "c");
            Assert.AreEqual(exist4, false);
            var exist5 = _set.Exists("a", "b", "d");
            Assert.AreEqual(exist5, false);

            var rsp3 = _set.Assert("x", "y", "z");
            var exists6 = _set.Exists("x", "y", "z");
            Assert.AreEqual(exists6, true);

            var exists7 = _set.Exists("x", "y", TripleObject.FromData("z"));
            Assert.AreEqual(exists7, false);
        }

        [TestMethod]
        public void GetBy_S_Returns()
        {
            _set.Assert("s1", "b2", "c2");
            _set.Assert("s1", "b3", TripleObject.FromData("c3"));
            _set.Assert("s1", "b1", "c1");

            _set.Assert("s2", "b2", "c2");
            _set.Assert("s2", "b3", "c3");

            _set.Assert("s3", "b1", "c1");

            var byS1 = _set.S("s1").ToArray();
            Assert.AreEqual(byS1.Count(), 3);
            CollectionAssert.Contains(byS1, new Triple("s1", "b1", "c1"));
            CollectionAssert.Contains(byS1, new Triple("s1", "b2", "c2"));
            CollectionAssert.Contains(byS1, new Triple("s1", "b3", TripleObject.FromData("c3")));

            var byS2 = _set.S("s2").ToArray();
            Assert.AreEqual(byS2.Count(), 2);
            CollectionAssert.Contains(byS2, new Triple("s2", "b2", "c2"));
            CollectionAssert.Contains(byS2, new Triple("s2", "b3", "c3"));

            var byS3 = _set.S("s3").ToArray();
            Assert.AreEqual(byS3.Count(), 1);
            CollectionAssert.Contains(byS3, new Triple("s3", "b1", "c1"));

            var byS4 = _set.S("s4").ToArray();
            Assert.AreEqual(byS4.Count(), 0);
        }


        [TestMethod]
        public void GetBy_SP_Returns()
        {
            _set.Assert("s2", "b2", "c2");
            _set.Assert("s2", "b2", "c2"); // duplicate assert
            _set.Assert("s2", "b3", "c3");

            _set.Assert("s1", "b2", ("c21", 0));
            _set.Assert("s1", "b2", ("c22", 1));
            _set.Assert("s1", "b3", TripleObject.FromData("c3"));
            _set.Assert("s1", "b1", "c1");

            _set.Assert("s3", "b1", "c1");

            var byS1B2 = _set.SP("s1", "b2").ToArray();
            Assert.AreEqual(byS1B2.Count(), 2);
            CollectionAssert.Contains(byS1B2, new Triple("s1", "b2", ("c21", 0)));
            CollectionAssert.Contains(byS1B2, new Triple("s1", "b2", ("c22", 1)));

            var byS1B3 = _set.SP("s1", "b3").ToArray();
            Assert.AreEqual(byS1B3.Count(), 1);
            CollectionAssert.Contains(byS1B3, new Triple("s1", "b3", TripleObject.FromData("c3")));

            var byS1B4 = _set.SP("s1", "b4").ToArray();
            Assert.AreEqual(byS1B4.Count(), 0);

            var byS1B0 = _set.SP("s1", "b0").ToArray();
            Assert.AreEqual(byS1B0.Count(), 0);

            var byS0B1 = _set.SP("s0", "b1").ToArray();
            Assert.AreEqual(byS0B1.Count(), 0);
        }

        [TestMethod]
        public void GetBy_P_Returns()
        {
            _set.Assert("s1", "p1", "o1");
            _set.Assert("s1", "p1", TripleObject.FromData("o2"));
            _set.Assert("s2", "p1", "c1");

            _set.Assert("s2", "p3", "c2");
            _set.Assert("s2", "p3", "c3");
            _set.Assert("s2", "p3", "c3"); // duplicate assert

            _set.Assert("s3", "p5", "c1");

            var byP1 = _set.P("p1").ToArray();
            Assert.AreEqual(byP1.Count(), 3);
            CollectionAssert.Contains(byP1, new Triple("s1", "p1", "o1"));
            CollectionAssert.Contains(byP1, new Triple("s2", "p1", "c1"));
            CollectionAssert.Contains(byP1, new Triple("s1", "p1", TripleObject.FromData("o2")));

            var byP3 = _set.P("p3").ToArray();
            Assert.AreEqual(byP3.Count(), 2);
            CollectionAssert.Contains(byP3, new Triple("s2", "p3", "c2"));
            CollectionAssert.Contains(byP3, new Triple("s2", "p3", "c3"));

            var byP5 = _set.P("p5").ToArray();
            Assert.AreEqual(byP5.Count(), 1);
            CollectionAssert.Contains(byP5, new Triple("s3", "p5", "c1"));

            var byP0 = _set.P("p0").ToArray();
            Assert.AreEqual(byP0.Count(), 0);

            var byP7 = _set.P("p7").ToArray();
            Assert.AreEqual(byP7.Count(), 0);
        }

        [TestMethod]
        public void Get_P_Returns()
        {
            _set.Assert("s1", "p1", "o1");
            _set.Assert("s1", "p1", TripleObject.FromData("o1"));
            _set.Assert("s2", "p1", "c1");

            _set.Assert("s2", "p3", "c2");
            _set.Assert("s2", "p3", "c3");
            _set.Assert("s2", "p3", "c3"); // duplicate assert

            _set.Assert("s3", "p5", "c1");

            var p = _set.P().ToArray();
            Assert.AreEqual(p.Count(), 3);
            CollectionAssert.Contains(p, "p1");
            CollectionAssert.Contains(p, "p3");
            CollectionAssert.Contains(p, "p5");
        }

        [TestMethod]
        public void GetBy_PO_Returns()
        {
            _set.Assert("s1", "p1", "o1");
            _set.Assert("s1", "p1", TripleObject.FromData("o3"));
            _set.Assert("s2", "p1", TripleObject.FromData("o3"));
            _set.Assert("s2", "p1", "o2");
            _set.Assert("s3", "p1", "o2");

            _set.Assert("s2", "p3", "c2");
            _set.Assert("s2", "p3", "c3");
            _set.Assert("s2", "p3", "c3"); // duplicate assert

            _set.Assert("s3", "p5", "c1");

            var p1o1 = _set.PO("p1", "o1").ToArray();
            Assert.AreEqual(p1o1.Count(), 1);
            CollectionAssert.Contains(p1o1, new Triple("s1", "p1", "o1"));

            var p1o1v = _set.PO("p1", TripleObject.FromData("o3")).ToArray();
            Assert.AreEqual(p1o1v.Count(), 2);
            CollectionAssert.Contains(p1o1v, new Triple("s1", "p1", TripleObject.FromData("o3")));
            CollectionAssert.Contains(p1o1v, new Triple("s2", "p1", TripleObject.FromData("o3")));

            var p3c3 = _set.PO("p3", "c3").ToArray();
            Assert.AreEqual(p3c3.Count(), 1);
            CollectionAssert.Contains(p3c3, new Triple("s2", "p3", "c3"));

            var byP0 = _set.PO("p0", "s1").ToArray();
            Assert.AreEqual(byP0.Count(), 0);

            var byP7 = _set.PO("p7", "s1").ToArray();
            Assert.AreEqual(byP7.Count(), 0);
        }

        [TestMethod]
        public void GetBy_O_Returns()
        {
            _set.Assert("s1", "p1", "o1");
            _set.Assert("s1", "p1", TripleObject.FromId("o3"));
            _set.Assert("s2", "p1", TripleObject.FromId("o3"));
            _set.Assert("s2", "p1", "o2");
            _set.Assert("s3", "p1", "o2");

            _set.Assert("s2", "p3", "c2");
            _set.Assert("s2", "p3", "c3");
            _set.Assert("s2", "p3", "c3"); // duplicate assert

            _set.Assert("s3", "p5", "c1");

            var o1 = _set.O("o1").ToArray();
            Assert.AreEqual(o1.Count(), 1);
            CollectionAssert.Contains(o1, new Triple("s1", "p1", "o1"));

            var o1v = _set.O(TripleObject.FromId("o3")).ToArray();
            Assert.AreEqual(o1v.Count(), 2);
            CollectionAssert.Contains(o1v, new Triple("s1", "p1", TripleObject.FromId("o3")));
            CollectionAssert.Contains(o1v, new Triple("s2", "p1", TripleObject.FromId("o3")));

            var c3 = _set.O("c3").ToArray();
            Assert.AreEqual(c3.Count(), 1);
            CollectionAssert.Contains(c3, new Triple("s2", "p3", "c3"));

            var c0 = _set.O("c0").ToArray();
            Assert.AreEqual(c0.Count(), 0);

            var c5 = _set.O("c5").ToArray();
            Assert.AreEqual(c5.Count(), 0);
        }

        [TestMethod]
        public void GetBy_OS_Returns()
        {
            _set.Assert("s1", "p1", "o1");
            _set.Assert("s1", "p2", "o1");
            _set.Assert("s1", "p3", "o1");
            _set.Assert("s1", "p1", TripleObject.FromId("o2"));
            _set.Assert("s1", "p2", TripleObject.FromId("o2"));
            _set.Assert("s2", "p1", TripleObject.FromId("o2"));
            _set.Assert("s2", "p1", "o2");
            _set.Assert("s3", "p1", "o2");

            _set.Assert("s2", "p3", "c2");
            _set.Assert("s2", "p3", "c3");
            _set.Assert("s2", "p3", "c3"); // duplicate assert

            _set.Assert("s3", "p5", "c1");

            var o1s1 = _set.OS("o1", "s1").ToArray();
            Assert.AreEqual(o1s1.Count(), 3);
            CollectionAssert.Contains(o1s1, new Triple("s1", "p1", "o1"));
            CollectionAssert.Contains(o1s1, new Triple("s1", "p2", "o1"));
            CollectionAssert.Contains(o1s1, new Triple("s1", "p3", "o1"));

            var o1vs1 = _set.OS(TripleObject.FromId("o2"), "s1").ToArray();
            Assert.AreEqual(o1vs1.Count(), 2);
            CollectionAssert.Contains(o1vs1, new Triple("s1", "p1", TripleObject.FromId("o2")));
            CollectionAssert.Contains(o1vs1, new Triple("s1", "p2", TripleObject.FromId("o2")));

            var c3s2 = _set.OS("c3", "s2").ToArray();
            Assert.AreEqual(c3s2.Count(), 1);
            CollectionAssert.Contains(c3s2, new Triple("s2", "p3", "c3"));

            var o0s1 = _set.OS("o0", "s1").ToArray();
            Assert.AreEqual(o0s1.Count(), 0);

            var o7s2 = _set.OS("o7", "s2").ToArray();
            Assert.AreEqual(o7s2.Count(), 0);
        }
    }
}
