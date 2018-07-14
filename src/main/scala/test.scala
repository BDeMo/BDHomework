import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.{JSON, JSONObject}
object test {
  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("LauncherStreaming")
    //    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //    ssc.checkpoint("checkpoint")
    //
    //
    //    val topic = args(0)
    //    val topicMap = topic.split(",").map((_,1)).toMap
    //    val kafkaStream = KafkaUtils.createStream(ssc,
    //      "hdp-node-01:2181,hdp-node-02:2181,hdp-node-03:2181,hdp-node-04:2181",
    //      "testing",
    //      topicMap
    //    ).map(_._2)
    //
    //    println("----------Start-------------")
    //    kafkaStream.foreachRDD(operat(_))
    //    println("----------------------------")
    //    ssc.start()
    //    ssc.awaitTermination()
    operat2("{\"data\":[{\"activetime\":398257,\"package\":\"app40\"},{\"activetime\":227591,\"package\":\"app56\"}],\"endtime\":1531305600000,\"begintime\":1531305000000,\"userId\":0,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":247588,\"package\":\"app77\"},{\"activetime\":199972,\"package\":\"app48\"},{\"activetime\":57593,\"package\":\"app26\"},{\"activetime\":8938,\"package\":\"app56\"},{\"activetime\":1528,\"package\":\"app43\"}],\"endtime\":1531306200000,\"begintime\":1531305600000,\"userId\":1,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":293918,\"package\":\"app57\"},{\"activetime\":22539,\"package\":\"app16\"},{\"activetime\":5096,\"package\":\"app80\"},{\"activetime\":76,\"package\":\"app93\"},{\"activetime\":51,\"package\":\"app21\"},{\"activetime\":19,\"package\":\"app92\"},{\"activetime\":14,\"package\":\"app29\"},{\"activetime\":2,\"package\":\"app45\"},{\"activetime\":0,\"package\":\"app55\"},{\"activetime\":0,\"package\":\"app13\"}],\"endtime\":1531306800000,\"begintime\":1531306200000,\"userId\":2,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":554928,\"package\":\"app59\"},{\"activetime\":368961,\"package\":\"app12\"},{\"activetime\":119043,\"package\":\"app32\"}],\"endtime\":1531307400000,\"begintime\":1531306800000,\"userId\":3,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":64338,\"package\":\"app36\"},{\"activetime\":28113,\"package\":\"app60\"},{\"activetime\":15400,\"package\":\"app77\"},{\"activetime\":4186,\"package\":\"app96\"},{\"activetime\":3074,\"package\":\"app35\"},{\"activetime\":1507,\"package\":\"app80\"},{\"activetime\":497,\"package\":\"app55\"},{\"activetime\":72,\"package\":\"app86\"}],\"endtime\":1531308000000,\"begintime\":1531307400000,\"userId\":4,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":268617,\"package\":\"app45\"}],\"endtime\":1531308600000,\"begintime\":1531308000000,\"userId\":5,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":358122,\"package\":\"app70\"},{\"activetime\":260312,\"package\":\"app84\"},{\"activetime\":103690,\"package\":\"app90\"},{\"activetime\":30127,\"package\":\"app6\"},{\"activetime\":5383,\"package\":\"app80\"},{\"activetime\":430,\"package\":\"app91\"},{\"activetime\":44,\"package\":\"app4\"},{\"activetime\":42,\"package\":\"app25\"},{\"activetime\":33,\"package\":\"app34\"}],\"endtime\":1531309200000,\"begintime\":1531308600000,\"userId\":6,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":213374,\"package\":\"app97\"},{\"activetime\":108356,\"package\":\"app78\"},{\"activetime\":106740,\"package\":\"app11\"},{\"activetime\":13995,\"package\":\"app88\"},{\"activetime\":9236,\"package\":\"app25\"},{\"activetime\":1851,\"package\":\"app85\"},{\"activetime\":1009,\"package\":\"app6\"}],\"endtime\":1531309800000,\"begintime\":1531309200000,\"userId\":7,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":384738,\"package\":\"app85\"}],\"endtime\":1531310400000,\"begintime\":1531309800000,\"userId\":8,\"day\":\"2018-07-11\"}\n{\"data\":[{\"activetime\":233630,\"package\":\"app91\"},{\"activetime\":207016,\"package\":\"app88\"},{\"activetime\":174099,\"package\":\"app2\"},{\"activetime\":101494,\"package\":\"app13\"},{\"activetime\":6379,\"package\":\"app26\"},{\"activetime\":2149,\"package\":\"app61\"},{\"activetime\":1748,\"package\":\"app77\"},{\"activetime\":761,\"package\":\"app37\"}],\"endtime\":1531311000000,\"begintime\":1531310400000,\"userId\":9,\"day\":\"2018-07-11\"}")
  }

  def operat(rdd: RDD[String]): Unit = {
    val str: String = rdd.toString()
    var jso = JSON.parseRaw(str)
  }

  def operat2(str: String): Unit = {
    val json = new JsonParser()
    for (tmp <- str.split("\n")) {
      val jp = new JsonParser()
      var jso = jp.parse(tmp).asInstanceOf[JsonObject]
      println(jso)
      println(tranTimeToString(jso.get("begintime").toString))
      println(tranTimeToDay(jso.get("begintime").toString))
      println((jso.get("data")).getAsJsonArray,jso.get("data").getAsJsonArray.size())
      println((jso.get("data")).getAsJsonArray.get(0).getAsJsonObject.get("package").toString.split("\"")(1))
      println(jso.get("day").toString.split("\"")(1))
    }
  }
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
  def tranTimeToDay(tm:String) :String={
    val fm = new SimpleDateFormat("dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
