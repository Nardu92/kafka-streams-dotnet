﻿using System;
using System.Runtime.Serialization;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Indicates a pre run time error occurred while parsing the <see cref="Topology"/> logical topology
    /// to construct the <see cref="ProcessorTopology"/> physical processor topology.
    /// </summary>
    [Serializable]
    public class TopologyException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public TopologyException(string message)
            : base(message)
        {
        }

        public TopologyException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Constructor used for deserialization of the exception
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected TopologyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
