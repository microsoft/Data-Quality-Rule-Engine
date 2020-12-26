// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.ms.dq.support

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

trait SupportTrait {
  //returns the dataframe:data ordered according to the list:orderedList
  def correctFormat(data: Dataset[Row],orderedList:List[String]): Dataset[Row]={
    data.select(orderedList.head,orderedList.tail: _*)
  }
  //inserts dataframe:writable into a praticular table:tableName
  def writeTableAppendAbsolute(writable: Dataset[Row], tableName: String) = {
    writable.write.mode(SaveMode.Append).insertInto(tableName)
  }
  //checks if a string:value is present in a string or not in a list:list irrespective of its case
  def containsIgnoreCase(list:Seq[String],value:String): Boolean={
    list.exists(item => item.toLowerCase() == value.toLowerCase())
  }

  def containsIgnoreCase(list:Seq[String],value:Seq[String]): Boolean={
    var inp=""
    for (inp <- value)
    {
      if(containsIgnoreCase(list,inp) == false)
        {
          println(inp + " is not present as a column in the dataframe")
          return false
        }
    }
    return true
  }

  def getStringFromSeq(stringList: Seq[String]): String = {
    stringList.map(a=>a).mkString(",")
  }
  //the function helps identifying the last time the table for a particular source and entity was processed , for delta processing
  def ruleWaterMarkStart(source: String, entity: String,rule: String, spark:SparkSession, properties: Properties): String = {
    import spark.implicits._
    var d: Dataset[Row] = null
    try {
      d = spark.table(properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RULE_WATERMARK_TABLE_NAME")).filter($"SubjectArea" === source).filter($"SourceEntityName" === entity).filter($"RuleName" === rule).agg(max("WaterMarkEndValue").alias("startValue"))
      if (!d.take(1).isEmpty) {
        if (null == d.first().getString(0)) {
          "17530101"
        } else {
          d.first().getString(0)
        }
      }
      else
        "17530101"
    }
    catch {
      case ex: Exception => println("Error with Watermark look up. "+ex.toString())
        throw ex
    }
  }
  //function to help in delta processing and returns all records in table name whose watermark value is greater than the start value and less than end value
  def dqDeltaIdentifier(tableName: String, auditCol: String,waterMarkStart:String,waterMarkEnd: String,spark:SparkSession):Dataset[Row]={
    try{
      if(!spark.catalog.tableExists(tableName))
      {
        println("TableName=>"+tableName+" does not exist. Please check metadata")
        throw new Exception("Table Does not exist")
      }
      val tableDf=spark.table(tableName)
      if (!containsIgnoreCase(tableDf.columns.toSeq, auditCol)) {
        println("TableName=>"+tableName+" does not contain column-->"+auditCol+". Please check metadata")
        throw new Exception(" Column Does not Exist")
      }
      if(waterMarkEnd.equals(Long.MaxValue.toString()))
        {
          tableDf.filter(col(auditCol)>waterMarkStart.toLong)
        }
      else
        {
          tableDf.filter(col(auditCol)>waterMarkStart.toLong).filter(col(auditCol)<waterMarkEnd.toLong)
        }
    }
    catch{
      case ex: Exception => println("Error with Delta Identifier. "+ex.toString())
        throw ex
    }
  }
  //function to help in delta processing and returns all records in table name whose watermark value is greater than the start value
  def dqDeltaIdentifier(tableName: String, auditCol: String,waterMarkStart:String,spark:SparkSession):Dataset[Row]={
    try{
      if(!spark.catalog.tableExists(tableName))
        {
          println("TableName=>"+tableName+" does not exist. Please check metadata")
          throw new Exception("Table Does not exist")
        }
      val tableDf=spark.table(tableName)
      if (!containsIgnoreCase(tableDf.columns.toSeq, auditCol)) {
        println("TableName=>"+tableName+" does not contain column-->"+auditCol+". Please check metadata")
        throw new Exception(" Column Does not Exist")
      }
        tableDf.filter(col(auditCol)>waterMarkStart.toLong)
    }
    catch{
      case ex: Exception => println("Error with Delta Identifier. "+ex.toString())
        throw ex
    }
  }
  //the function helps in updating the watermark value of the table for a particular source and entity , for delta processing
  def updateRuleWaterMark(source: String, entity: String, rule: String, waterMarkEndVal: String,spark: SparkSession,properties: Properties)={
    try{
      val selectExpression= "Select '" + source + "' as SubjectArea, '" + entity + "' as SourceEntityName, '" + rule + "' as RuleName, '17530101' as WaterMarkStartValue, '" + waterMarkEndVal + "' as WaterMarkEndValue, date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
      spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RULE_WATERMARK_TABLE_NAME") + " "+selectExpression)
    }
    catch{
      case ex: Exception => println("Error with Watermark Update. "+ex.toString())
        throw ex
    }
  }
  //get properties from application.properties
  def getProperties(): Properties = {
    val url = getClass().getResource("/application.properties")
    val properties: Properties = new Properties()

    if (url != null) {
      val source = scala.io.Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      println("Properties file cannot be loaded")
      throw new java.io.FileNotFoundException("Properties file cannot be loaded");
    }

    return properties
  }
}
