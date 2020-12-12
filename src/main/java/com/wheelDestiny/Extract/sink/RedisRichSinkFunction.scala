package com.wheelDestiny.Extract.sink

import java.util

import com.wheelDestiny.Extract.redis.JedisConnectionPool
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{Jedis, Transaction}

import scala.collection.mutable.ArrayBuffer


/**
 * redis的存储逻辑
 */
class RedisRichSinkFunction extends RichSinkFunction[RedisRecord] {

  var redis: Jedis = _

  override def invoke(value: RedisRecord, context: SinkFunction.Context[_]): Unit = {

    //把正反规则存到redis中,使用redis事务
    val trueRule: String = value.trueRule
    val failRule: util.ArrayList[String] = value.failRule
    val host: String = value.host
    if (!trueRule.equals("") || !failRule.isEmpty) {
      redis = JedisConnectionPool.getConnection()
      val transaction: Transaction = redis.multi()
      //切换数据库
      transaction.select(11)

      // 正规则存总值和有序集合，总值累加key的前缀为total开头，反规则存自动排序的集合
      // 正规则Key的前缀为txpath开头

      if (!trueRule.equals("")) {
        transaction.incr(s"total:${host}")
        transaction.zincrby(s"txpath:${host}", 1, trueRule)
      }

      //反规则存集合，Key的前缀为fxpath开头
      if (!failRule.isEmpty) {
        import scala.collection.convert.wrapAll._
        transaction.sadd(s"fxpath:${host}", failRule: _*)
      }
      transaction.exec()
      redis.close()
    }
  }
}

//存入redis的数据类型
case class RedisRecord(var host:String,var trueRule:String,var failRule:util.ArrayList[String])


