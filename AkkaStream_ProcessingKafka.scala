// https://github.com/PacktPublishing/Akka-Cookbook/blob/master/Chapter08/src/main/scala/com/packt/chapter8/ProcessingKafkaApplication.scala


import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import oleg_test_scalaxb.Passenger
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.duration._
import scala.xml.{Elem, XML}



object ProcessingKafkaApplication  extends App {

  implicit val actorSystem = ActorSystem("SimpleStream")
  implicit val actorMaterializer = ActorMaterializer()

  val bootstrapServers = "localhost:9092"
  val kafkaTopic = "kafka_stream"
  val partition = 0
  val subscription = Subscriptions.assignment(new TopicPartition(kafkaTopic, partition))

  val consumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("akka_streams_group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  //val producerSettings = ProducerSettings(actorSystem, new ByteArraySerializer, new StringSerializer)
  //  .withBootstrapServers(bootstrapServers)

  val runnableGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    //val tickSource = Source.tick(0 seconds, 5 seconds, "Hello from Akka Streams using Kafka!")
    val kafkaSource = Consumer.plainSource(consumerSettings, subscription)
    //val kafkaSink = Producer.plainSink(producerSettings)
    val printlnSink = Sink.foreach[String]( xml =>{
      //println(xml)
      val record_xml: Elem = XML.loadString(xml.toString())

      val xml_case_class = scalaxb.fromXML[oleg_test_scalaxb.PnrRetrieveResponse](record_xml)
      xml_case_class.pnrResponse.passengers.Passenger.foreach({
        case Passenger(headCount, passengerType, passengerFullName, individualName) => println(individualName.getOrElse(""))
      })
    })

    //val mapToProducerRecord = Flow[String].map(elem => new ProducerRecord[Array[Byte], String](kafkaTopic, elem))
    val mapFromConsumerRecord = Flow[ConsumerRecord[Array[Byte], String]].map(record => record.value())

    //tickSource  ~> mapToProducerRecord   ~> kafkaSink
    kafkaSource ~> mapFromConsumerRecord ~> printlnSink

    ClosedShape
  })

  runnableGraph.run()



}
