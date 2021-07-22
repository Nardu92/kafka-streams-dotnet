using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    public interface ITopicCreatorManager : IDisposable
    {
        IAdminClient AdminClient { get; }

        Task<IEnumerable<string>> CreateNewTopics(IDictionary<string, InternalTopicConfig> topics);

        List<string> GetTopicsToCreate(IDictionary<string, InternalTopicConfig> topics, Metadata metadata);

        int GetNumberPartitionForTopic(Metadata metadata, string topicName);
    }
}
