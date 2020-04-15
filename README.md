# Kafka Stream .NET

Proof of Concept : Kafka Stream Implementation for .NET Application [WORK IN PROGRESS]

It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka)

I need contribution ;)

# Stateless processor implemention

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DONE|
|---|---|---|---|---|---|
|Branch|KStream -> KStream[]|   |   |&#9745;|   |
|Filter|KStream -> KStream|   |   |&#9745;|   |
|Filter|KTable -> KTable|   |   |&#9745;|   |
|InverseFilter|KStream -> KStream|   |   |&#9745;|   |
|InverseFilter|KTable -> KTable|   |   |&#9745;|   |
|FlatMap|KStream → KStream|   |   |&#9745;|   |
|FlatMapValues|KStream → KStream|   |   |&#9745;|   |
|Foreach|KStream → void|   |   |&#9745;|   |
|GroupByKey|KStream → KGroupedStream|   |   |&#9745;|   |
|GroupBy|KStream → KGroupedStream|   |   |&#9745;|   |
|GroupBy|KTable → KGroupedTable|   |   |&#9745;|   |
|Map|KStream → KStream|   |   |&#9745;|   |
|MapValues|KStream → KStream|   |   |&#9745;|   |
|MapValues|KTable → KTable|   |   |&#9745;|   |
|Peek|KStream → KStream|   |   |&#9745;|   |
|Print|KStream → void|   |   |&#9745;|   |
|SelectKey|KStream → KStream|   |   |&#9745;|   |
|Table to Steam|KTable → KStream|   |   |&#9745;|   |

# Statefull processor implementation

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DONE|
|---|---|---|---|---|---|
|Aggregate|KGroupedStream -> KTable|&#9745;|   |   |   |
|Aggregate|KGroupedTable -> KTable|&#9745;|   |   |   |
|Aggregate(windowed)|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedTable -> KTable|&#9745;|   |   |   |
|Count(windowed)|KGroupedStream → KStream|&#9745;|   |   |   |
|Reduce|KGroupedStream → KTable|&#9745;|   |   |   |
|Reduce|KGroupedTable → KTable|&#9745;|   |   |   |
|Reduce(windowed)|KGroupedStream → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|OuterJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|LeftJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|OuterJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |

TODO : Processor API

# Test topology driver

Must be used for testing your stream topology. Simulate a kafka cluster in memory.
Usage: 
```
static void Main(string[] args)
{
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-test-driver-app";
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .Filter((k, v) => v.Contains("test"))
        .To("test-output");

    Topology t = builder.Build();

    using (var driver = new TopologyTestDriver(t, config))
    {
        var inputTopic = driver.CreateInputTopic<string, string>("test");
        var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
        inputTopic.PipeInput("test", "test-1234");
        var r = outputTopic.ReadKeyValue();
    }
}
```

# Priority feature for stateless beta version

- [EOS](https://github.com/LGouellec/kafka-stream-net/issues/2) [ ]

# TODO implementation

- Statefull processors impl [ ]
- Subtopology impl [ ]
- Task restoring [ ]
- Topology description [ ]
- Global state store [ ]
- Processor API [ ] + Refactor topology node processor builder [ ]
- Repartition impl [ ]
- Unit test (TestTopologyDriver, ...) [ ]
- [EOS](https://github.com/LGouellec/kafka-stream-net/issues/2) [ ]
- Rocks DB state implementation [ ]
- Optimizing Kafka Streams Topologies  [ ]
- Interactive Queries [ ]
- Metrics [ ]

Some documentations for help during implementation :
https://docs.confluent.io/current/streams/index.html
https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#stateless-transformations

# Usage

Sample code
```
static void Main(string[] args)
{
    CancellationTokenSource source = new CancellationTokenSource();
    
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-app";
    config.BootstrapServers = "192.168.56.1:9092";
    config.SaslMechanism = SaslMechanism.Plain;
    config.SaslUsername = "admin";
    config.SaslPassword = "admin";
    config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
    config.AutoOffsetReset = AutoOffsetReset.Earliest;
    config.NumStreamThreads = 2;
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .FilterNot((k, v) => v.Contains("test"))
        .Peek((k,v) => Console.WriteLine($"Key : {k} | Value : {v}"))
        .To("test-output");

    builder.Table(
        "test-ktable",
        StreamOptions.Create(),
        InMemory<string, string>.As("test-ktable-store"));

    Topology t = builder.Build();
    KafkaStream stream = new KafkaStream(t, config);

    Console.CancelKeyPress += (o, e) => {
        source.Cancel();
        stream.Close();
    };

    stream.Start(source.Token);
}
```