package com.wheelDestiny.Extract.util

import java.sql.{Connection, DriverManager}

import com.wheelDestiny.Extract.conf.MyConfig.MYSQL_CONFIG

object JDBCUtil {
  classOf[com.mysql.jdbc.Driver]

  def getConnection: Connection = {
    DriverManager.getConnection(MYSQL_CONFIG("url"), MYSQL_CONFIG("username"),MYSQL_CONFIG("password"))
  }
}
