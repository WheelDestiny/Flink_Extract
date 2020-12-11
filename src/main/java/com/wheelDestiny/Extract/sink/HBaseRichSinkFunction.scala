package com.wheelDestiny.Extract.sink

import com.wheelDestiny.Extract.conf.MyConfig
import com.wheelDestiny.Extract.util.{JavaUtil, Record, Util}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * hbase的存储逻辑
 */
class HBaseRichSinkFunction extends RichSinkFunction[HBaseRecord] {

  var connection: Connection = _
  var table: Table = _

  override def open(parameters: Configuration) = {
    val hbaseConf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    connection = ConnectionFactory.createConnection(hbaseConf)
    table = connection.getTable(TableName.valueOf("wheeldestiny:context_extract"))
  }

  override def close() = {
    table.close()
    connection.close()
  }

  override def invoke(value: HBaseRecord, context: SinkFunction.Context[_]) = {
    // hbase
    //create 'context_extract',{NAME => 'i', VERSIONS => 1, BLOCKCACHE => true,COMPRESSION => 'SNAPPY'},
    // {NAME => 'c', VERSIONS => 1, BLOCKCACHE => true,COMPRESSION => 'SNAPPY'},
    // {NAME => 'h', VERSIONS => 1,COMPRESSION => 'SNAPPY'}
    val put = new Put(Bytes.toBytes(s"${value.host}_${Util.getTime(value.eventTime, "yyyyMMddHHmmssSSSS")}_${value.urlMd5}"))
    if (JavaUtil.isNotEmpty(value.url)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("url"), Bytes.toBytes(value.url))
    if (JavaUtil.isNotEmpty(value.domain)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("domain"), Bytes.toBytes(value.domain))
    if (JavaUtil.isNotEmpty(value.host)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("host"), Bytes.toBytes(value.host))
    if (JavaUtil.isNotEmpty(value.content)) put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("context"), Bytes.toBytes(value.content))
    if (JavaUtil.isNotEmpty(value.html)) put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("html"), Bytes.toBytes(value.html))
    print("导入HBase")
    table.put(put)
  }
}

case class HBaseRecord(url: String, host: String, content: String, domain: String, urlMd5: String, eventTime: Long, html: String) extends Record
