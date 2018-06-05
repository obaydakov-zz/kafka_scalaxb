package oelg_test.scalaxb

import java.io.File

import oleg_test_scalaxb._
import oleg_test_scalaxb.PnrRetrieveResponse

import scala.xml.{Elem, XML}
import java.util.Properties
import java.util.Arrays

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

//object KeyValueImplicits {

//  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

//}

object scalaxb_test extends App with StreamToTableJoinTestData {

//  RaadXmlFile2CaseClass
//  Kafka_Spark
  KafkaStreaming

  def KafkaStreaming()= {

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_stream")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      // Specify default (de)serializers for record keys and for record values.
      settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    val stringSerde: Serde[String] = Serdes.String()

    val builder = new KStreamBuilder
    val textLines: KStream[Array[Byte], String] = builder.stream("kafka_stream")

    val originalAndUppercased: KStream[String, String] = textLines.typesafe.map(mapper = ( key, value ) => {
      //println(value)
      val record_xml: Elem = XML.loadString(value.toString())
      //println(records_xml)
      val xml_case_class = scalaxb.fromXML[oleg_test_scalaxb.PnrRetrieveResponse](record_xml)
      //println(parsedAddress)
      xml_case_class.pnrResponse.passengers.Passenger.foreach({
        case Passenger(headCount, passengerType, passengerFullName, individualName) => println(individualName.getOrElse(""))
      })
      (value, value.toUpperCase())
    })

    val streams = new KafkaStreams(builder, streamingConfig)
    streams.start()
  }

  def Kafka_Spark()={
  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming")
    .master("local[2]")
    // .master("spark://127.0.01:7077")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  val df = spark
    .readStream
    .format("kafka")
    //   .option("kafka.bootstrap.servers", "dolinux808.hq.emirates.com:9192")
    //   .option("subscribe", "HLX_MARS_TIB_KFT_EKPNRDATA_IN_ETC")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "spark_streaming_test")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  import spark.implicits._

  val counts = df.selectExpr("CAST(value AS STRING)")

    val writer = new ForeachWriter[Row] {
      var myPartition: Option[Long] = None
      var myVersion: Option[Long] = None

      override def open ( partitionId: Long, version: Long ): Boolean = {

        myPartition = Some(partitionId)
        myVersion = Some(version)
        println(s"*** ForEachWriter: open partition=[$myPartition] version=[$myVersion]")

       // FileUtils.forceMkdir(new File(s"/tmp/example/${partitionId}"))
       // fileWriter = new FileWriter(new File(s"/tmp/example/${partitionId}/temp"))

        val processThisOne = version == 1
        processThisOne
      }

      override def process ( record: Row ): Unit = {
        //println(s"*** ForEachWriter: process partition=[$myPartition] version=[$myVersion] record=$record")
        //[null,<xml><a>asdasdad</a><b>qweqweqwe</b></xml>]
        //println(record.toString().drop(1).dropRight(1))
        val record_xml: Elem =XML.loadString(record.toString().drop(1).dropRight(1))
        val XML_case_class = scalaxb.fromXML[oleg_test_scalaxb.PnrRetrieveResponse](record_xml)

        XML_case_class.pnrResponse.passengers.Passenger.foreach({
          case Passenger(headCount, passengerType,passengerFullName,individualName) => println(individualName.getOrElse(""))
        })
      }

      override def close ( errorOrNull: Throwable ): Unit = {
        println(s"*** ForEachWriter: close partition=[$myPartition] version=[$myVersion]")
        myPartition = None
        myVersion = None
      }
    }

    val query = counts.writeStream.format("console")
      .foreach(writer).start()

/*  val query = counts.writeStream.format("console")
    .foreach(new ForeachWriter[Row] {

      var myPartition: Option[Long] = None
      var myVersion: Option[Long] = None

      override def open ( partitionId: Long, version: Long ): Boolean = {
        myPartition = Some(partitionId)
        myVersion = Some(version)
        val processThisOne = version == 1
        processThisOne
      }

        override def process ( record: Row ): Unit = {
        //println(s"*** ForEachWriter: process partition=[$myPartition] version=[$myVersion] record=$record")
        //[null,<xml><a>asdasdad</a><b>qweqweqwe</b></xml>]
        //println(record.toString().drop(1).dropRight(1))
        val record_xml: Elem =XML.loadString(record.toString().drop(1).dropRight(1))
        val XML_case_class = scalaxb.fromXML[oleg_test_scalaxb.PnrRetrieveResponse](record_xml)

        XML_case_class.pnrResponse.passengers.Passenger.foreach({
          case Passenger(headCount, passengerType,passengerFullName,individualName) => println(individualName.getOrElse(""))
        })
      }

     override def close ( errorOrNull: Throwable ): Unit = {
        println(s"*** ForEachWriter: close partition=[$myPartition] version=[$myVersion]")
        myPartition = None
        myVersion = None
      }
    }).start() */

  query.awaitTermination()
  spark.stop()
}

  def RaadXmlFile2CaseClass()= {
    val classLoader = getClass.getClassLoader
    //val path:String="C:\\Users\\S754344\\Documents\\HelixNG_NEW\\scalaxb_test\\src\\main\\resources\\PNR.xml"
    val xml = XML.loadFile(classLoader.getResource("PNR.xml").getFile)

    val PNR_case_class = scalaxb.fromXML[PnrRetrieveResponse](xml)



    PNR_case_class.pnrResponse.passengers.Passenger.foreach({
      case Passenger(headCount, passengerType, passengerFullName, individualName) => println(individualName.getOrElse(""))
    })
  }




}
