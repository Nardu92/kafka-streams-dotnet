using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Tests.Errors
{
    public class ErrorsDeserializationTests
    {
        [Test]
        public void TestDeserializationExceptionDeserialization()
        {
            Exception ex = new DeserializationException("Message", new Exception("Inner exception."));
            // Save the full ToString() value, including the exception message and stack trace.
            string exceptionToString = ex.ToString();

            // Round-trip the exception: Serialize and de-serialize with a BinaryFormatter
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                // "Save" object state
                bf.Serialize(ms, ex);
                // Re-use the same stream for de-serialization
                ms.Seek(0, 0);
                // Replace the original exception with de-serialized one
                ex = (DeserializationException)bf.Deserialize(ms);
            }

            // Double-check that the exception message and stack trace (owned by the base Exception) are preserved
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestIllegalStateExceptionDeserialization()
        {
            Exception ex = new IllegalStateException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (IllegalStateException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestInvalidStateStoreExceptionDeserialization()
        {
            Exception ex = new InvalidStateStoreException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (InvalidStateStoreException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestNotMoreValueExceptionDeserialization()
        {
            Exception ex = new NotMoreValueException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (NotMoreValueException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestProcessorStateExceptionDeserialization()
        {
            Exception ex = new ProcessorStateException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (ProcessorStateException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestProductionExceptionDeserialization()
        {
            Exception ex = new ProductionException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (ProductionException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestStreamConfigExceptionDeserialization()
        {
            Exception ex = new StreamConfigException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (StreamConfigException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestStreamsExceptionDeserialization()
        {
            Exception ex = new StreamsException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (StreamsException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestTaskMigratedExceptionDeserialization()
        {
            Exception ex = new TaskMigratedException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (TaskMigratedException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }

        [Test]
        public void TestTopologyExceptionDeserialization()
        {
            Exception ex = new TopologyException("Message", new Exception("Inner exception."));
            string exceptionToString = ex.ToString();

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, ex);
                ms.Seek(0, 0);
                ex = (TopologyException)bf.Deserialize(ms);
            }
            Assert.AreEqual(exceptionToString, ex.ToString(), "ex.ToString()");
        }
    }


}
