using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class DefaultTopicManagerTests
    {
        [Test]
        public async Task TestDefaultTopicManagerApply()
        {
            var topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });

            var topicCreatorManagerMoq = new Mock<ITopicCreatorManager>();
            topicCreatorManagerMoq.Setup(x => x.CreateNewTopics(topics))
                .ReturnsAsync(new List<string>() { "topic" });

            DefaultTopicManager manager = new DefaultTopicManager(topicCreatorManagerMoq.Object);

            var r = (await manager.ApplyAsync(topics)).ToList();

            Assert.AreEqual(1, r.Count);
            topicCreatorManagerMoq.VerifyAll();
        }

        [Test]
        public async Task TestDefaultTopicManagerApplyThrows()
        {
            var topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });

            var topicCreatorManagerMoq = new Mock<ITopicCreatorManager>();
            topicCreatorManagerMoq.Setup(x => x.CreateNewTopics(topics))
                .ThrowsAsync(new StreamsException("Exception"));


            DefaultTopicManager manager = new DefaultTopicManager(topicCreatorManagerMoq.Object);

            Assert.ThrowsAsync<StreamsException>(async () => await manager.ApplyAsync(topics));

            topicCreatorManagerMoq.Verify(x => x.CreateNewTopics(topics), Times.Exactly(10));
        }
    }
}