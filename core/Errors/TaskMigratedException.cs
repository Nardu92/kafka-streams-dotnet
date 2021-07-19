using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    [Serializable]
    public class TaskMigratedException : Exception
    {
        public TaskMigratedException(string message)
            : base(message)
        {
        }

        public TaskMigratedException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Constructor used for deserialization of the exception
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected TaskMigratedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
