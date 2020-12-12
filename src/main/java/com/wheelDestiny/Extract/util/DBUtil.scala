package com.wheelDestiny.Extract.util

import java.sql.{Statement, Timestamp, Connection => jsc}
import java.util.Date

import org.apache.commons.codec.digest.DigestUtils

object DBUtil {

  def insertIntoMysqlByJdbc(record: MysqlRecord): Unit = {
    var connection: jsc = null
    var statement: Statement = null
    try {
      val date = new Date()
      val time: Long = date.getTime
      val timestamp = new Timestamp(time)
      val hour_md5: String = DigestUtils.md5Hex(Util.getTime(time, "yyyyMMddHH"))
      val scanDay: Int = Util.getTime(time, "yyyyMMdd").toInt
      val scanHour: Int = Util.getTime(time, "HH").toInt
      connection = JDBCUtil.getConnection
      connection.setAutoCommit(false)
      statement = connection.createStatement()
      val sql =
        s"""
           |insert into wheelDestiny_report_stream_extract
           |(host, scan, filtered, extract, empty_context, no_match_xpath, scan_day, scan_hour, scan_time, hour_md5)
           |values('${record.host}', ${record.scan}, ${record.filtered}, ${record.extract}, ${record.emptyContext}, ${record.noMatchXpath}, $scanDay, $scanHour, '$timestamp','$hour_md5')
           |on DUPLICATE KEY UPDATE
           |scan=scan+${record.scan},
           |filtered=filtered+${record.filtered},
           |extract=extract+${record.extract},
           |empty_context=empty_context+${record.emptyContext},
           |no_match_xpath=no_match_xpath+${record.noMatchXpath};
            """.stripMargin
      statement.execute(sql)

      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        try {
          connection.rollback()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    } finally {
      try {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

  }

}
