using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Production exception throw when production message is in error and <see cref="IStreamConfig.ProductionExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
    /// </summary>
    [Serializable]
    public class ProductionException : Exception
    {
        /// <summary>
        /// Production exception throw when production message is in error and <see cref="IStreamConfig.ProductionExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
        /// </summary>
        /// <param name="message">Exception message</param>
        public ProductionException(string message)
            : base(message)
        {
        }
        public ProductionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Constructor used for deserialization of the exception
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected ProductionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
