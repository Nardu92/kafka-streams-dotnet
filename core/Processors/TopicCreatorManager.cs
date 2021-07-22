using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Processors
{
    public class TopicCreatorManager : ITopicCreatorManager
    {
        private readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        private readonly ILog log = Logger.GetLogger(typeof(TopicCreatorManager));
        public IAdminClient AdminClient { get; private set; }
        private readonly IStreamConfig config;

        public TopicCreatorManager(IStreamConfig config, IAdminClient adminClient)
        {
            this.config = config;
            this.AdminClient = adminClient;
        }

        public async Task<IEnumerable<string>> CreateNewTopics(IDictionary<string, InternalTopicConfig> topics)
        {
            var defaultConfig = new Dictionary<string, string>();
            var metadata = AdminClient.GetMetadata(timeout);
            var topicsToCreate = GetTopicsToCreate(topics, metadata);

            if (topicsToCreate.Any())
            {
                var topicSpecifications = topicsToCreate
                    .Select(t => topics[t])
                    .Select(t => new TopicSpecification()
                    {
                        Name = t.Name,
                        NumPartitions = t.NumberPartitions,
                        ReplicationFactor = (short)config.ReplicationFactor,
                        Configs = new Dictionary<string, string>(t.GetProperties(defaultConfig, config.WindowStoreChangelogAdditionalRetentionMs))
                    });

                await AdminClient.CreateTopicsAsync(topicSpecifications);

                // Check if topics has been created
                var newTopicsCount = AdminClient.GetMetadata(timeout).Topics.Select(t => t.Topic).Intersect(topicsToCreate).Count();
                if (newTopicsCount != topicsToCreate.Count)
                {
                    throw new StreamsException($"Not all topics have not been created. Please retry.");
                }
                else
                {
                    log.Debug($"Internal topics has been created : {string.Join(", ", topicsToCreate)}");
                }
            }

            log.Debug($"Complete to apply internal topics in topic manager.");
            return topicsToCreate;
        }

        public List<string> GetTopicsToCreate(IDictionary<string, InternalTopicConfig> topics, Metadata metadata)
        {
            // 1. get source topic partition
            // 2. check if changelog exist, :
            //   2.1 - if yes and partition number exactly same; continue;
            //   2.2 - if yes, and partition number !=; throw Exception
            // 3. if changelog doesn't exist, create it with partition number and configuration
            var topicsToCreate = new List<string>();
            foreach (var t in topics)
            {
                var numberPartitions = GetNumberPartitionForTopic(metadata, t.Key);
                if (numberPartitions == 0)
                {
                    // Topic need to create
                    topicsToCreate.Add(t.Key);
                }
                else
                {
                    if (numberPartitions == t.Value.NumberPartitions)
                    {
                        continue;
                    }
                    else
                    {
                        string msg = $"Existing internal topic {t.Key} with invalid partitions: expected {t.Value.NumberPartitions}, actual: {numberPartitions}. Please clean up invalid topics before processing.";
                        log.Error(msg);
                        throw new StreamsException(msg);
                    }
                }
            }
            return topicsToCreate;
        }


        public int GetNumberPartitionForTopic(Metadata metadata, string topicName)
        {
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topicName));
            return topicMetadata != null ? topicMetadata.Partitions.Count : 0;
        }

        public void Dispose()
        {
            AdminClient.Dispose();
        }
    }
}
