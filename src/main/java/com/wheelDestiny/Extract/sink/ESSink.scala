package com.wheelDestiny.Extract.sink

import java.net.{InetAddress, InetSocketAddress}

import com.wheelDestiny.Extract.util.Record
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * es的存储逻辑
  */
object ESSink{
  def apply(): ElasticsearchSink[ESRecord] ={
    val esClusterName = "hainiu-es"
    val esPort = 9300
    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    val esConfig = new java.util.HashMap[String, String]
    esConfig.put("cluster.name", esClusterName)
    // This instructs the sink to emit after every element, otherwise they would be buffered
    esConfig.put("bulk.flush.max.actions", "100")

    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s1.hadoop"), esPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s2.hadoop"), esPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s3.hadoop"), esPort))
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s4.hadoop"),esPort))
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s5.hadoop"),esPort))
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s6.hadoop"),esPort))
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s7.hadoop"),esPort))
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s8.hadoop"),esPort))

    val esSink = new ElasticsearchSink[ESRecord](esConfig, transportAddresses, new ElasticsearchSinkFunction[ESRecord] {

      def createIndexRequest(element: ESRecord): IndexRequest = {
        val map = new java.util.HashMap[String, String]
        map.put("url", element.url)
        map.put("url_md5", element.urlMd5)
        map.put("host", element.host)
        map.put("domain", element.domain)
        map.put("date", element.eventTime.toString)
        map.put("content", element.content)

        //
        print(s"输出到elasticSearch")
        Requests.indexRequest()
          .index("wheeldestiny_flink")
          .`type`("MainExtractor")
          .source(map)
      }

      override def process(t: ESRecord,
                           runtimeContext: RuntimeContext,
                           requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndexRequest(t))
      }
    })
    esSink
  }
}
//es数据类型
case class ESRecord(url: String, host: String, content: String, domain: String, urlMd5: String, eventTime: Long) extends Record