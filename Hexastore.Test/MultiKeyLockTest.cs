using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hexastore.Processor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hexastore.Test
{
    [TestClass]
    public class MultiKeyLockProcessorTest : RocksFixture
    {
        private object[] _items;
        private MultiKeyLockFactory factoryUnderTest;

        public MultiKeyLockProcessorTest()
        {
           factoryUnderTest = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 500);
        }


        [TestMethod]
        public void Single_Key_Gets_Lock()
        {
            var keys = new[] {"testKey1"};
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(keys))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Distinct_Key_Gets_Lock()
        {
            var keys = new[] { "testKey1", "testKey2" };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(keys))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Identical_Key_Times_Out()
        {
            var keys = new[] { "testKey1", "testKey1" };
            var passed = false;
            try
            {
                using (var mlock = factoryUnderTest.WriteLock(keys))
                {
                    passed = false;
                }
            }
            catch (TimeoutException)
            {
                passed = true;
            }
            
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Single_Key_Sync_Gets_Lock()
        {
            var keys1 = new[] { "testKey1" };
            var keys2 = new[] { "testKey1" };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(keys1))
            {
            }
            using (var mlock = factoryUnderTest.WriteLock(keys2))
            {
                passed = true;
            }
            Assert.IsTrue(passed);
        }

        [TestMethod]
        public void Multi_Key_Sync_Gets_Lock()
        {
            var keys1 = new[] { "testKey1", "testKey2" };
            var keys2 = new[] { "testKey1", "testKey2" };
            var passed = false;
            using (var mlock = factoryUnderTest.WriteLock(keys1))
            {
            }
            using (var mlock = factoryUnderTest.WriteLock(keys2))
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
            var keys = new List<string[]>();

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new string[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = $"{i}.{k}";
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];

            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(keys[t], 2000, MultiKeyLockFactory);
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
            var keys = new List<string[]>();

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new string[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = $"{k}";
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];
            var longerTimeoutFactory = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 10000);
            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(keys[t], 20, longerTimeoutFactory);
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
            var keys = new List<string[]>();
            var passed = false;

            for (var i = 0; i < numTasks; i++)
            {
                var keyArray = new string[numKeys];
                for (var k = 0; k < numKeys; k++)
                {
                    keyArray[k] = $"{k}";
                }
                keys.Add(keyArray);
            }

            Task[] tasks = new Task[numTasks];
            var longerTimeoutFactory = new MultiKeyLockFactory(new ConcurrentDictionary<string, byte>(), 1000);
            for (int t = 0; t < numTasks; t++)
            {
                var tws = new TaskWithState(keys[t], 200, longerTimeoutFactory);
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
            private string[] keys;
            private int spinMs;
            private IMultiKeyLockFactory factory;

            public TaskWithState(string[] keys, int spinMs, IMultiKeyLockFactory factory)
            {
                this.keys = keys;
                this.spinMs = spinMs;
                this.factory = factory;
            }

            public void TryGetLock()
            {
                using (var mLock = factory.WriteLock(keys))
                {
                    Thread.Sleep(spinMs);
                }
            }
        }
    }
}
