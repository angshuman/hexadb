using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hexastore.Graph;
using Hexastore.Processor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hexastore.Test
{
    [TestClass]
    public class MultiKeyLockProcessorTest : RocksFixture
    {
        private MultiKeyLockFactory factoryUnderTest;
        private string storeId;

        public MultiKeyLockProcessorTest()
        {
            factoryUnderTest = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 500);
            storeId = "teststore";
        }


        [TestMethod]
        public void Single_Key_Gets_Lock()
        {
            var triples = new[] { new Triple("test", "key", "val") };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(storeId, triples))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Distinct_Key_Gets_Lock()
        {
            var triples = new[] { new Triple("test", "key1", "val"), new Triple("test", "key2", "val") };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(storeId, triples))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Identical_Key_Gets_Lock()
        {
            var triples = new[] { new Triple("test", "key1", "val"), new Triple("test", "key1", "val") };
            var passed = false;

            using (var mlock = factoryUnderTest.WriteLock(storeId, triples))
            {
                passed = true;
            }

            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Single_Key_Sync_Gets_Lock()
        {
            var keys1 = new[] { new Triple("test", "key1", "val") };
            var keys2 = new[] { new Triple("test", "key1", "val") };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(storeId, keys1))
            {
            }
            using (var mlock = factoryUnderTest.WriteLock(storeId, keys2))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Key_Sync_Gets_Lock()
        {
            var keys1 = new[] { new Triple("test", "key1", "val"), new Triple("test", "key2", "val") };
            var keys2 = new[] { new Triple("test", "key1", "val"), new Triple("test", "key2", "val") };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(storeId, keys1))
            {
            }
            using (var mlock = factoryUnderTest.WriteLock(storeId, keys2))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Key_Async_Distinct_Gets_Lock()
        {
            var numTasks = 32;
            var numKeys = 500;
            var keys = new List<Triple[]>();

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new Triple[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = new Triple($"{i}", $"{k}", "val");
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];

            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(storeId, keys[t], 2000, MultiKeyLockFactory);
                tasks[t] = new Task(tws.TryGetLock);
            }

            foreach (Task task in tasks)
            {
                task.Start();
            }

            foreach (Task task in tasks)
            {
                task.Wait();
            }

            // If no Task time out. success
        }

        [TestMethod]
        public void Multi_Key_Async_Identical_Gets_Lock()
        {
            var numTasks = 32;
            var numKeys = 500;
            var keys = new List<Triple[]>();

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new Triple[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = new Triple($"{k}", $"{k}", "val");
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];
            var longerTimeoutFactory = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 10000);
            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(storeId, keys[t], 20, longerTimeoutFactory);
                tasks[t] = new Task(tws.TryGetLock);
            }

            foreach (Task task in tasks)
            {
                task.Start();
            }

            foreach (Task task in tasks)
            {
                task.Wait();
            }

            // If no Task time out. success
        }

        [TestMethod]
        public void Multi_Key_Async_Identical_Times_Out()
        {
            var numTasks = 32;
            var numKeys = 500;
            var keys = new List<Triple[]>();
            var passed = false;

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new Triple[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = new Triple($"{k}", $"{k}", "val");
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];
            var longerTimeoutFactory = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 1000);
            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(storeId, keys[t], 200, longerTimeoutFactory);
                tasks[t] = new Task(tws.TryGetLock);
            }

            try
            {
                foreach (Task task in tasks)
                {
                    task.Start();
                }

                foreach (Task task in tasks)
                {
                    task.Wait();
                }
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException.GetType() == typeof(TimeoutException))
                {
                    passed = true;
                }
            }

            Assert.IsTrue(passed);
        }

        public class TaskWithState
        {
            private Triple[] keys;
            private int spinMs;
            private IMultiKeyLockFactory factory;
            private string storeId;

            public TaskWithState(string storeId, Triple[] keys, int spinMs, IMultiKeyLockFactory factory)
            {
                this.storeId = storeId;
                this.keys = keys;
                this.spinMs = spinMs;
                this.factory = factory;
            }

            public void TryGetLock()
            {
                using (var mLock = factory.WriteLock(storeId, keys))
                {
                    Thread.Sleep(spinMs);
                }
            }
        }
    }
}
