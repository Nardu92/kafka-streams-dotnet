using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TopicCreatorManagerTests
    {
        private SyncKafkaSupplier kafkaSupplier;

        [SetUp]
        public void Begin()
        {
            kafkaSupplier = new SyncKafkaSupplier();
        }

        [TearDown]
        public void Dispose()
        {
            kafkaSupplier = null;
        }

        private TopicCreatorManager GetTopicCreatorManager()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            var topicCreatorManager = new TopicCreatorManager(config2, kafkaSupplier.GetAdmin(config));
            return topicCreatorManager;
        }

        [Test]
        public async Task ApplyInternalChangelogTopics()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = (await topicCreatorManager.CreateNewTopics(topics)).ToList();

            Assert.AreEqual(2, r.Count);
            Assert.AreEqual("topic", r[0]);
            Assert.AreEqual("topic1", r[1]);
        }

        [Test]
        public async Task ApplyInternalChangelogTopics2()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new WindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new WindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = (await topicCreatorManager.CreateNewTopics(topics)).ToList();

            Assert.AreEqual(2, r.Count);
            Assert.AreEqual("topic", r[0]);
            Assert.AreEqual("topic1", r[1]);
        }

        [Test]
        public async Task ApplyInternalChangelogTopicsWithExistingTopics()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = (await topicCreatorManager.CreateNewTopics(topics)).ToList();

            Assert.AreEqual(1, r.Count);
            Assert.AreEqual("topic1", r[0]);
        }

        [Test]
        public async Task ApplyInternalChangelogTopicsInvalidDifferentPartition()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            // Create topic with just one partition
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 4
            });

            Assert.ThrowsAsync<StreamsException>(async () => await topicCreatorManager.CreateNewTopics(topics));
        }

        [Test]
        public void ApplyInternalChangelogTopicsParrallel()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });

            var r = Parallel.ForEach(new List<int> { 1, 2, 3, 4 }, async (i) => await topicCreatorManager.CreateNewTopics(topics));
            Assert.IsTrue(r.IsCompleted);
        }

        [Test]
        public void TestTopicCreatorManagerGetTopicsToCreate()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            // Create topic with just one partition
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic1");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });
            topics.Add("topic2", new UnwindowedChangelogTopicConfig
            {
                Name = "topic2",
                NumberPartitions = 1
            });
            topics.Add("topic3", new UnwindowedChangelogTopicConfig
            {
                Name = "topic3",
                NumberPartitions = 1
            });
            var metadata = topicCreatorManager.AdminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var result = topicCreatorManager.GetTopicsToCreate(topics, metadata);
            Assert.AreEqual(2, result.Count());
            Assert.Contains("topic2", result);
            Assert.Contains("topic3", result);
        }

        [Test]
        public void TestTopicCreatorManagerGetTopicsToCreateNoNewTopics()
        {
            TopicCreatorManager topicCreatorManager = GetTopicCreatorManager();

            // Create topic with just one partition
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic1");
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic2");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });
            topics.Add("topic2", new UnwindowedChangelogTopicConfig
            {
                Name = "topic2",
                NumberPartitions = 1
            });
            var metadata = topicCreatorManager.AdminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var result = topicCreatorManager.GetTopicsToCreate(topics, metadata);
            Assert.IsEmpty(result);
        }

        [Test]
        public void TestTopicCreatorManagerGetTopicsToCreateDifferentPartitionsThrows()
        {
            var topicCreatorManager = GetTopicCreatorManager();

            // Create topic with just one partition
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic1");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 2
            });

            var metadata = topicCreatorManager.AdminClient.GetMetadata(TimeSpan.FromSeconds(10));
            Assert.Throws<StreamsException>(() => topicCreatorManager.GetTopicsToCreate(topics, metadata));

        }
    }
}