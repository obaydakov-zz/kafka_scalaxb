package oelg_test.scalaxb

import java.io.File

import oleg_test_scalaxb._
import oleg_test_scalaxb.PnrRetrieveResponse

import scala.xml.{Elem, XML}
import java.util.Properties
import java.util.Arrays

import oelg_test.scalaxb.scalaxb_test.{userClicksTopic, userRegionsTopic}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{KeyValueMapper, Materialized, ValueMapper}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import util.{EmbeddedKafkaServer, SimpleKafkaClient, SparkKafkaSink}
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
//import util.{EmbeddedKafkaServer, SimpleKafkaClient}

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

import org.apache.kafka.streams.KeyValue

import fr.psug.kafka.streams.KafkaStreamsImplicits._

import com.lightbend.kafka.scala.streams._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced, Serialized}
import org.apache.kafka.streams._

import java.lang.{Long => JLong}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}

import org.apache.kafka.streams.kstream.ValueMapper
import java.util

import org.apache.kafka.streams.kstream.Reducer

import scala.language.implicitConversions

import ImplicitConversions._

import org.apache.kafka.streams.kstream.{KStreamBuilder, KeyValueMapper}

import collection.JavaConverters.asJavaIterableConverter

import org.apache.kafka.streams.kstream.KStream
import java.util

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.StreamsBuilder


object KafkaStreams_Examples {


 /* def StreamToTableJoinScalaIntegration_Lightbend() ={
    // https://github.com/lightbend/kafka-streams-scala/blob/develop/src/test/scala/com/lightbend/kafka/scala/streams/StreamToTableJoinScalaIntegrationTestImplicitSerdes.scala
    import DefaultSerdes._

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG,
        s"stream-table-join-scala-integration-test-implicit-ser-${scala.util.Random.nextInt(100)}")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "join-scala-integration-test-implicit-ser-standard-consumer")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
      p
    }

    val builder = new StreamsBuilderS()

    val userClicksStream: KStreamS[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTableS[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTableS[String, Long] =
      userClicksStream
        // Join the stream against the table.
        .leftJoin(userRegionsTable,
        (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)
        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    clicksPerRegion.mapValues(value =>println(value))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()


  }

  def KafkaStream_shouldCountWords_MapExample() {
    // To convert between Scala's `Tuple2` and Streams' `KeyValue`.
    //import KeyValueImplicits._

    val inputTextLines: Seq[String] = Seq(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit"
    )
    //  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

    val expectedWordCounts: Seq[KeyValue[String, Long]] = Seq(
      ("hello", 1L),
      ("all", 1L),
      ("streams", 2L),
      ("lead", 1L),
      ("to", 1L),
      ("join", 1L),
      ("kafka", 3L),
      ("summit", 1L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // Use a temporary directory for storing state, which will be automatically removed after the test.

      p
    }

    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()

    val builder: StreamsBuilder = new StreamsBuilder()

    val inputTopic = "inputTopic"
    val outputTopic = "output-topic"
    // Construct a `KStream` from the input topic, where message values represent lines of text (for
    // the sake of this example, we ignore whatever may be stored in the message keys).
    val textLines: KStream[String, String] = builder.stream(inputTopic)

    // Scala-Java interoperability: to convert `scala.collection.Iterable` to  `java.util.Iterable`
    // in `flatMapValues()` below.


    // Works
    textLines.mapValues[Double](new ValueMapper[AnyRef, Double]() {
      override def apply(value: AnyRef) = 6.3
    })

    val wordCounts:KTable[String, JLong] =textLines.mapValues[Iterable[String]](new ValueMapper[String, Iterable[String]]() {
      override def apply (value: String): Iterable[String] = {
        Arrays.asList[Array[String]](value.toLowerCase().split("\\W+"))
      }}).groupBy(( _, word:String ) => word).count()


    val helloStream: KStream[String, String] = textLines.mapValues(new ValueMapper[String, String]{
      override def apply(value: String): String = s"hello $value"
    })

    // Two examples from https://github.com/knoldus/kafka-streams-scala-examples/blob/master/src/main/scala/com/knoldus/kafka/examples/AggregationExample.scala
    val mappedStream =
      textLines.map[String, Integer] {
        new KeyValueMapper[String, String, KeyValue[String, Integer]] {
          override def apply(key: String, value: String): KeyValue[String, Integer] = {
            new KeyValue(key, new Integer(value.length))
          }
        }
      }
    val mappedStream =
      textLines.flatMapValues[String] {
        new ValueMapper[String, java.lang.Iterable[java.lang.String]]() {
          override def apply(value: String): java.lang.Iterable[java.lang.String] = {
            value.toLowerCase.split("\\W+").toIterable.asJava
          }
        }
      }.groupBy {
        new KeyValueMapper[String, String, String]() {
          override def apply(key: String, word: String): String = word
        }
      }.count("Counts")

    // Two examples from https://index.scala-lang.org/ogirardot/typesafe-kafka-streams/typesafe-kafka-streams-11/0.2.3?target=_2.12
    textLines.typesafe.map((k, v) => (k,s"hello $v"))
    textLines.typesafe.filter((k, v) => k.startsWith("valid_")).flatMapValues(value =>value.toLowerCase().split("\\W+"))


    // example from https://www.michael-noll.com/blog/2018/04/05/of-stream-and-tables-in-kafka-and-stream-processing-part1/
    val builder1 = new StreamsBuilder
    val stream1 = builder1.stream("input-topic", Consumed.`with`(Serdes.String, Serdes.String))
    val stream2 = stream1.map { case (k: Array[Byte], v: Array[Byte]) => new String(k) -> new String(v) }
    // val table = stream1.typesafe.groupByKey().reduce((aggV, newV) -> newV);
    //           .map { case (k, v) => (k, v.reduceLeft( (aggV, newV) => newV)._2) }

    wordCounts.toStream.to(outputTopic, Produced.`with`(stringSerde, longSerde))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()
  }
 */

}
