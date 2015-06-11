# Netty Based Apache Kafka Producer [![Build Status](https://travis-ci.org/milenkovicm/netty-kafka-producer.svg)](https://travis-ci.org/milenkovicm/netty-kafka-producer)


A very short POC to see if it is possible to make netty based kafka producer which utilizes netty's off-heap buffer allocators.  

At the moment producer is very limited. It can connect to brokers and send messages to them, and that is more or less end of it's capabilities. 
There is basic support for broker or connection failures. As this is only POC please expect many rough edges which need further attention. 

If you need battle tested kafka producer the the original one is better fit at the moment.

Producer was tested with apache kafka 0.8.2.1.

## How to use

It is simple to use, and it is asynchronous: 

```java
ByteBuf key = ...
ByteBuf message = ...
ProducerProperties properties = new ProducerProperties();
KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties);
producer.connect().sync();
KafkaTopic kafkaTopic = producer.topic();
kafkaTopic.send(key,message);
```

To redefine how producer partitions data you have to implement `Partitioner`, which is similar to original kafka partitioner interface.

```java
public class RRPartitioner extends Partitioner {
    @Override
    public int partition(ByteBuf key, int numberOfPartitions) {
        return Math.abs(key.getByte(0) % numberOfPartitions);
    }
}
```

Next step is to update producer properties to include new partitioner:

```java
ProducerProperties properties = new ProducerProperties();
properties.override(ProducerProperties.PARTITIONER, new RRPartitioner());

KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties);
```

Netty producer has no runtime dependency on any kafka or scala package.

## Copy vs composite 

Producer handler comes in two flavours, `CopyProducerHandler` which allocates new `ByteBuf` per producer request and copies original event to it, 
and `CompositeProducerHandler` which uses `CompositeByteBuf` to wrap message before sending. Early tests indicate that `CopyProducerHandler` has more than
2.5 folds smaller heap footprint, with negligible off heap increase.

## Producer properties

Producer has three types of `ProducerProperties`, `Netty`, `Kafka` and network specific configuration. All properties are type safe.
If no property specified producer will use sensible defaults.

## Message acknowledgment

If producer is configured with `Acknowledgment.WAIT_FOR_ALL_REPLICAS` or `Acknowledgment.WAIT_FOR_LEADER` it would be possible 
to listen for server acknowledgments. 

```java
  final Future<Void> future = topic.send(key,message);
  future.addListener(new FutureListener<Void>() {
      @Override
      public void operationComplete(Future<Void> future) throws Exception {
          // handle message ack 
      }
  })
```

`io.netty.channel.Future<Void>` will be completed when server acknowledges receipt of message.
In case of write failures `Future` will be completed with `isSuccess` `false`. 
When `Acknowledgment.WAIT_FOR_NO_ONE` is selected no acknowledgment will be available and netty will select to use 
`channel.voidPromise()` which means no write failures could be detected. 

By default `Acknowledgment.WAIT_FOR_LEADER` is selected. To change it: 

```java
ProducerProperties properties = new ProducerProperties();
properties.override(ProducerProperties.DATA_ACK, Acknowledgment.WAIT_FOR_NO_ONE);
KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties)
```

## Slow broker handling

By default producer has application scope buffers borrowed from netty, which may compensate slow broker. 
Configuring buffer watermarks is easy like:

```java
ProducerProperties properties = new ProducerProperties();
properties.override(ProducerProperties.NETTY_HIGH_WATERMARK, 200_000);
properties.override(ProducerProperties.NETTY_LOW_WATERMARK, 100_000);

KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties)
```
 
In case of buffer overflow there is configurable application protection:
  
```java
ProducerProperties properties = new ProducerProperties();
properties.override(ProducerProperties.BACKOFF_STRATEGY, new DropBackoffStrategy());

KafkaProducer producer = new KafkaProducer("localhost", 9092, "test_topic", properties)
```

`DropBackoffStrategy` would drop any message if application buffer is full. Other available strategy is `ParkBackoffStrategy` 
which would park sender thread for few nanoseconds and check if channel is writtable.


## Monitoring

Base set of topic metrics will be collected and exposed via JMX. (more detail to follow)


## Some internals 

Producer creates one `Channel` per broker involved in topic partitioning. 
Each `Channel` has it's own `EventLoopGroup` with only one`Thread` running. There may be future improvements in this department, as we may decide to share `EventLoopGroup` across all channels involved in data transport.
Also, create `Channel` per partition rather than `Channel` per broker, may be under consideration.
Producer also creates one `Channel` which will be used for metadata retrieval. Metadata channel connects to the host specified in producer constructor (for initial connection) or to some of active brokers (in cases of metadata connection failures). 

## Things coming

- improve broker error handling and better handling of broker disconnections.
- possibility to specify multiply brokers for metadata retrieval.

```java
KafkaProducer producer = new KafkaProducer("host1:9092,host2:9092,host3:9092", "test_topic", properties);
```

- support for message batching - you have to do batching yourself
- compression - no compression is available at the moment.
- (maybe) multi-topic support - currently is limited to only one topic.
- (maybe) ability for application to listen for topology changes.
- (maybe) per partition client connection - currently producer will establish one connection per broker. In some cases it may be useful to have connection per partition. 
- (maybe) shared `EventLoopGroup` with predefined number of treads - currently each connection will create `EventLoopGroup` limited to one thread. 


## Performance numbers

We didn't compare kafka native producer and netty based producer so far as it wouldn't be like to like comparison, netty based producer plays completely different role to us. 
Some early numbers show that netty producer writes to single broker around ~100MB/s generating less than 1MB/s of heap allocation, with lot of space for further improvement.

Few screenshots from runs we've done, pushing events to two partitions, where each partition resides on a different broker. We managed to push ~200MB/s (~100MB/s per partition) sending 128KB events.

(more performance test to follow)

CPU profile: 

![CPU profile](http://i.imgur.com/wAAyfFk.png "CPU&Memory profile")

I/O profile (bytes/sec):

![IO profile](http://i.imgur.com/3voInd6.png "IO Profile")

## Links

 - https://kafka.apache.org
 - https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
