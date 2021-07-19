using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    [Serializable]
    public class NotMoreValueException : Exception
    {
        public NotMoreValueException(string message)
            : base(message)
        {
        }

        public NotMoreValueException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Constructor used for deserialization of the exception
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected NotMoreValueException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
