﻿using System;
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
    public class DefaultTopicManager : ITopicManager
    {
        private readonly ILog log = Logger.GetLogger(typeof(DefaultTopicManager));
        private readonly IStreamConfig config;
        private readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        public DefaultTopicManager(IStreamConfig config, IAdminClient adminClient)
        {
            this.config = config;
            AdminClient = adminClient;
        }

        public IAdminClient AdminClient { get; private set; }

        /// <summary>
        /// <para>
        /// Prepares a set of given internal topics.
        /// </para>
        /// <para>
        /// If a topic does not exist creates a new topic.
        /// If a topic with the correct number of partitions exists ignores it.
        /// If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
        /// </para>
        /// </summary>
        /// <param name="topics">internal topics of topology</param>
        /// <returns>the list of topics which had to be newly created</returns>
        public async Task<IEnumerable<string>> ApplyAsync(IDictionary<string, InternalTopicConfig> topics)
        {
            Exception _e = null;
            int maxRetry = 10, i = 0;
            while (i < maxRetry)
            {
                try
                {
                    log.Debug($"Starting to apply internal topics in topic manager (try: {i + 1}, max retry : {maxRetry}).");
                    return await CreateNewTopics(topics);
                }
                catch (Exception e)
                {
                    ++i;
                    _e = e;
                    log.Debug($"Impossible to create all internal topics: {e.Message}. Maybe an another instance of your application just created them. (try: {i + 1}, max retry : {maxRetry})");
                }
            }

            throw new StreamsException(_e);
        }

        private async Task<IEnumerable<string>> CreateNewTopics(IDictionary<string, InternalTopicConfig> topics)
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

        private List<string> GetTopicsToCreate(IDictionary<string, InternalTopicConfig> topics, Metadata metadata)
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

        public void Dispose()
            => AdminClient.Dispose();

        private int GetNumberPartitionForTopic(Metadata metadata, string topicName)
        {
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topicName));
            return topicMetadata != null ? topicMetadata.Partitions.Count : 0;
        }

    }
}
