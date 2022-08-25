package clickstream

import java.util.Properties

import config.Settings
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

object LogProducer extends App {
  // WebLog config
  val wlc = Settings.WebLogGen

  //  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  //  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  val Users = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/user_info.csv")).getLines().toArray
  val Courses = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/course_info.csv")).getLines().toArray

  //  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  //  val Pages = (0 to wlc.pages).map("Page-" + _)

  val rnd = new Random()

  val topic = wlc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  for (fileCount <- 1 to wlc.numberOfFiles) {

    //val fw = new FileWriter(filePath, true)

    // introduce some randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis() // move all this to a function
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "pause_video"
        case 1 => "start_video"
        case _ => "stop_video"
      }
      val course = Courses(rnd.nextInt(Courses.length) - 1)
      val user = Users(rnd.nextInt(Users.length) - 1)
      val session_id = String.valueOf(rnd.nextInt(30000))


      //      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      //      val prevPage = referrer match {
      //        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
      //        case _ => ""
      //      }
      //      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      //      val page = Pages(rnd.nextInt(Pages.length - 1))
      //      val product = Products(rnd.nextInt(Products.length - 1))
      // {"course_id": "course-v1:TsinghuaX+30670043X+2016_T",
      // "user_id": "3134629",
      // "session_id": "3f0cf80bc0b9f32a6a381d615add11ef",
      // "activity_event": "stop_video",
      // "time": "2016-07-31T23:59:13"}
      val line = s"$adjustedTimestamp\t$course\t$user\t$action\t$session_id\n"
      val producerRecord = new ProducerRecord(topic, line)
      kafkaProducer.send(producerRecord)
      //fw.write(line)

      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }

    val sleeping = 2000
    println(s"Sleeping for $sleeping ms")
  }

  kafkaProducer.close()
}