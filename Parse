import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

object Parse {
  // 解析一行
  def parse(log : String) = {
    // 日志中的所有字段
    val all_fileds = Array("@timestamp", "http_host", "status", "scheme", "request", "method", "remote_addr",
      "request_time", "response_time", "body_bytes_sent", "http_referrer", "http_user_agent", "http_x_forwarded_for",
      "http_upstream", "cookie_id", "user_id", "session_id", "cookie_stock_flight", "@version", "type", "host",
      "path", "client_addr")
    // 解析json
    val j = log.parseJson.asInstanceOf[JsObject].fields
    val res = new scala.collection.mutable.ArrayBuffer[String]
    // 如果有某个字段就获取这个字段 否则为""
    //    if(j.contains("method")) {
    //      res.append(j("method").toString + "xxx")
    //    }
    for(field <- all_fileds) {
      if(j.contains(field)) {
        res.append( j(field).toString.replaceAll("\"",""))//.split("\"")(1))
      } else {
        res.append("")
      }
    }
    res.toArray
  }
  def main(args : Array[String]) {
    // 输入路径 输出路径
    val Array(input_path, output_path) = args
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.textFile(input_path).map{ case log => {
      //val alldata = sc.textFile("data/Flag/*/part-*")
      // 解析读到的每一行
      try {
        parse(log)
      } catch {
        //抛出异常情况
        case _ : Throwable => { Array[String]() }
      }
    } }.filter{ case ( arr ) => arr.size > 0 }
      .map(_.mkString("\t"))
      .repartition(20)
      //.saveAsTextFile("/home/andy/bb")		// 保存到输出路径
      .saveAsTextFile(output_path)
    sc.stop()
  }
}


