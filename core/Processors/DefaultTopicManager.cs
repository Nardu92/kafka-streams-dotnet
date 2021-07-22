using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
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
        private readonly ITopicCreatorManager topicCreatorManager;

        public DefaultTopicManager(ITopicCreatorManager topicCreatorManager)
        {
            this.topicCreatorManager = topicCreatorManager;
        }

        public IAdminClient AdminClient => topicCreatorManager.AdminClient;

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
                    return await topicCreatorManager.CreateNewTopics(topics);
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

        public void Dispose() => topicCreatorManager.Dispose();

    }
}
