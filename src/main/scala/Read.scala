import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._




object Read {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LauncherStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topic = args(0)
    val topicMap = topic.split(",").map((_,1)).toMap
    val kafkaStream = KafkaUtils.createStream(ssc,
      "hdp-node-01:2181,hdp-node-02:2181,hdp-node-03:2181,hdp-node-04:2181",
      "testing",
      topicMap
    ).map(_._2)

    println("----------Start-------------")
    kafkaStream.foreachRDD(operate(_))
    println("----------------------------")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 对每个收到数据RDD就行的操作
    * @param rdd
    */
  def operate(rdd : RDD[String]): Unit ={
    val arr = rdd.collect().array
    println("------------------------------------------------------------------------------------")
    println("-------------------" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "-----------------------")
    println("------------------------------------------------------------------------------------")
//    rdd.collect().foreach(println)
    val jp = new JsonParser()
    for (tmp <- arr) {
      var jso = jp.parse(tmp).asInstanceOf[JsonObject]
      println("Action write into :" + jso)
      var i = 0
      for(i <- 0 to jso.get("data").getAsJsonArray.size() - 1) {
        unzipTUHA(jso, i)//4
        unzipTUDA(jso, i)//1
        unzipTUH(jso, i)//2
        unzipTUD(jso, i)//3
      }
    }
    println("------------------------------------------------------------------------------------")
    println("------------------------------------------------------------------------------------")
  }

  val hBaseConfiguration = HBaseConfiguration.create()
  val hadmin = new HBaseAdmin(hBaseConfiguration)

  /**
    * 极好用的读取
    * @param rowkey
    * @param tableName
    * @param columnFamily
    * @param column
    * @return
    */
  def readFromHbase(rowkey:String,tableName: String, columnFamily: String,column:String): String = {
    println("__reading data")
    if (hadmin.tableExists(tableName) == false) {
      println("method: table " + tableName + " not exists " + tableName)

      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(columnFamily.getBytes()))
      hadmin.createTable(tableDesc)

      println("method:table"+ tableName + " builded")

      null
    } else {
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val table = connection.getTable(TableName.valueOf(tableName))
      val get = new Get(Bytes.toBytes(rowkey))
      val result = table.get(get)
      val resultvalue_temp = result.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(column))
      val resultvalue = Bytes.toString(resultvalue_temp)
      println("data read is:"+resultvalue)
      table.close()
      connection.close()

      resultvalue
    }

  }

  /**
    * 写入Hbase极好用的函数，Lol
    * @param tablename
    * @param rowKey
    * @param columnFamilyName
    * @param data
    */
  def writeToHbase(tablename: String, rowKey: String, columnFamilyName: String, data: (String, String)): Unit = {
    println("__inserting data")
    //如果该表不存在，新建表
    if (hadmin.tableExists(tablename) == false) {
      println("method: table " + tablename + " not exists " + tablename)

      val tableDesc = new HTableDescriptor(tablename)
      tableDesc.addFamily(new HColumnDescriptor(columnFamilyName.getBytes()))
      hadmin.createTable(tableDesc)

      println("method:table"+ tablename + " builded")

      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val table = connection.getTable(TableName.valueOf(tablename))

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(data._1), Bytes.toBytes(data._2))
      table.put(put)

      table.close()
      connection.close()

    } else {

      val connection =ConnectionFactory.createConnection(hBaseConfiguration)
      val table = connection.getTable(TableName.valueOf(tablename))
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(data._1), Bytes.toBytes(data._2))
      table.put(put)
      table.close()
      connection.close()
    }

  }

  /**
    * 第一个需求，用户某天用过哪些APP和使用时长
    *
    * @param jso
    * @param index
    */
  def unzipTUDA(jso : JsonObject, index : Int): Unit ={
    println("_______first unzipTUDA")
    val tmpjso = jso.get("data").getAsJsonArray.get(index).getAsJsonObject
    val tmpP = tmpjso.get("package").toString.split("\"")(1)
    var tmpV = tmpjso.get("activetime").toString.toLong
    val userId = jso.get("userId")
    val day = jso.get("day").toString.split("\"")(1)
    val begintime = jso.get("begintime").toString
    var value = readFromHbase(
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2),
      "behavior_user_app_201807",
      "timeLen",
      "timeLen."+tmpP)
    if(value != null){
      tmpV += value.toLong
    }
    writeToHbase("behavior_user_app_201807",
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2),
      "timeLen",
      ("timeLen."+tmpP,tmpV.toString))

  }

  /**
    * 第二个需求，用户每小时的玩机时长
    *
    * @param jso
    * @param index
    */
  def unzipTUH(jso : JsonObject, index : Int): Unit ={
    println("_______second unzipTUH")
    val tmpjso = jso.get("data").getAsJsonArray.get(index).getAsJsonObject
    val tmpP = tmpjso.get("package").toString.split("\"")(1)
    var tmpV = tmpjso.get("activetime").toString.toLong
    val userId = jso.get("userId")
    val day = jso.get("day").toString.split("\"")(1)
    val begintime = jso.get("begintime").toString
    var value = readFromHbase(
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2),
      "behavior_user_hour_time_201807",
      "timeLen",
      tranTimeToHour(begintime))
    if(value != null){
      tmpV += value.toLong
    }
    writeToHbase("behavior_user_hour_time_201807",
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2),
      "timeLen",
      (tranTimeToHour(begintime),tmpV.toString))

  }

  /**
    * 第三个需求，用户每天的玩机时长
    *
    * @param jso
    * @param index
    */
  def unzipTUD(jso : JsonObject, index : Int): Unit ={
    println("_______third unzipTUD")
    val tmpjso = jso.get("data").getAsJsonArray.get(index).getAsJsonObject
    val tmpP = tmpjso.get("package").toString.split("\"")(1)
    var tmpV = tmpjso.get("activetime").toString.toLong
    val userId = jso.get("userId")
    val day = jso.get("day").toString.split("\"")(1)
    val begintime = jso.get("begintime").toString
    var value = readFromHbase(
      userId.toString,
      "behavior_user_day_time_201807",
      "timeLen",
      "timeLen."+tranTimeToDay(begintime))
    if(value != null){
      tmpV += value.toLong
    }
    writeToHbase("behavior_user_day_time_201807",
      //        "userId:yyyymmdd:appname"
      userId.toString,
      "timeLen",
      ("timeLen."+tranTimeToDay(begintime),tmpV.toString))

  }

  /**
    * 第四个需求，用户每个应用每小时的玩机时长
    * @param jso
    * @param index
    */
  def unzipTUHA(jso : JsonObject, index : Int): Unit ={
    println("_______forth unzipTUHA")
    val tmpjso = jso.get("data").getAsJsonArray.get(index).getAsJsonObject
    val tmpP = tmpjso.get("package").toString.split("\"")(1)
    var tmpV = tmpjso.get("activetime").toString.toLong
    val userId = jso.get("userId")
    val day = jso.get("day").toString.split("\"")(1)
    val begintime = jso.get("begintime").toString
    var value = readFromHbase(
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2) +":"+  tmpP,
      "behavior_user_hour_app_time_201807",
      "timeLen",
      "timeLen."+tranTimeToHour(begintime))
    if(value != null){
      tmpV += value.toLong
    }
    writeToHbase("behavior_user_hour_app_time_201807",
      userId +":"+ day.split("-")(0) + day.split("-")(1)+ day.split("-")(2) +":"+  tmpP,
      "timeLen",
      ("timeLen."+tranTimeToHour(begintime),tmpV.toString))

  }

  def tranTimeToHour(tm:String) :String={
    val fm = new SimpleDateFormat("HH")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
  def tranTimeToDay(tm:String) :String={
    val fm = new SimpleDateFormat("dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
