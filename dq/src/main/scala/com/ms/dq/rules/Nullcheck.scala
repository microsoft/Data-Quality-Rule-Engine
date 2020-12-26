// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.ms.dq.rules

import com.ms.dq.support.SupportTrait
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import java.util.{Calendar, Date, Properties}
class Nullcheck extends SupportTrait{

  // Method to invoke null check on a dataset:df based on metadata entry
  // Returns the orginial dataframe along with additional flags indicating whether the required columns have passed/failed the null check for each particular record
  def apply(df: Dataset[Row], params: String, entityname: String, sourcename: String, spark: SparkSession, pipelineId: String, properties: Properties): Dataset[Row]={
    try {
      val ruleName = "nullcheck"
      if (params == null) {
        println("Skipping "+ruleName+". Please input Parameters for nullcheck on source=" + sourcename + " and entity=" + entityname + " in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      import spark.implicits._
      //getting the required paramaeters from the JSON:params to apply nullcheck (eg , columnList)
      val paramsSchema = List(
        StructField("params", StringType, false))
      val paramsRow = Seq(Row(params))
      val paramsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(paramsRow),
        StructType(paramsSchema)
      )
      val paramsString = paramsDf.select(col("params") as "params").map(_.toString())
      val readJson = spark.read.json(paramsString).asInstanceOf[Dataset[Row]]
      val readJsonCols = readJson.columns.toSeq
      //sanity check to validate json:params
      if (containsIgnoreCase(readJsonCols, "_corrupt_record")) {
        println("Skipping "+ruleName+". The Parameters for nullcheck on source=" + sourcename + " and entity=" + entityname + " are not a valid Json. Please input a valid Json in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //sanity check for required column:columnList in params
      if (!containsIgnoreCase(readJsonCols, "columnList")) {
        println("Skipping "+ruleName+". Mandatory Key \"columnList\" required in Parameters for nullcheck on source=" + sourcename + " and entity=" + entityname + ". Please make the required changes in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //Getting list of columns:distinctColList to apply nullcheck on
      val columns = readJson.select("columnList").first.getString(0)
      val colList = columns.split(",").toList
      val distinctColList = colList.distinct

      //columns of the dqfailtable
      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")
      var originalDfColumns = df.columns.toSeq
      if (containsIgnoreCase(originalDfColumns, "dq_uniqueID")) {
        originalDfColumns = originalDfColumns.filter(!_.contains("dq_uniqueID"))
      }
      //view name for the final result
      val view_uid = java.util.UUID.randomUUID.toString.replace('-', '_')
      val resultViewName = "vw_Result_" + view_uid

      //SQL queries for null check will come here
      var sqlexpression = ""
      //sql expression to get the failed records for nullcheck will come here
      var sqllogExpression = ""
      //sql expression to log relevant information about the run will come here
      var dimaggExpression = ""

      val recordCount = df.count()
      val colListAsString = getStringFromSeq(originalDfColumns)
      //traversing through all columns to apply null check
      for (colName <- distinctColList) {
        if (!containsIgnoreCase(originalDfColumns, colName)) {
          println("Skipping " + ruleName + " for column " + colName + " as it does not exist in frame provided")
        }
        else {
          val dqAggKey = java.util.UUID.randomUUID.toString
          sqlexpression = sqlexpression + ",case when " + colName + " is null then false else true end as " + colName + "_" + ruleName
          sqllogExpression = sqllogExpression + (if (sqllogExpression != "") " union all " else "") + "Select  to_json(struct(" + colListAsString + "))as Record,'" + sourcename + "' as Source,'" + entityname + "' as Entity,'" + colName + "' as ColumnName,'" + dqAggKey + "' as DqAggTableKey  from " + resultViewName + " where " + colName + "_" + ruleName + "= false"
          dimaggExpression += (if (dimaggExpression != "") " union all " else "") + "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + colName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId ,  date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        }
      }
      //sanity check if the null check expression was built successfully
      if (sqlexpression == "" || sqllogExpression == "" || dimaggExpression == "") {
        return df
      }
      //creating resulting dataframe with required DQ columns
      val inputViewName = "vw_Input_" + view_uid
      df.createOrReplaceTempView(inputViewName)
      val dqResultDf = spark.sql("select *" + sqlexpression + " from " + inputViewName)
      dqResultDf.createOrReplaceTempView(resultViewName)

      if (properties.getProperty("DQ_LOG_RESULTS_FLAG").toBoolean) {
        //logging results in required tables
        val failedresult = spark.sql(sqllogExpression)

        val current_time = current_timestamp()
        var failTable = failedresult.withColumn("Rule", lit(ruleName)).withColumn("PlatformModifiedDate", date_format(current_time, "y-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("PlatformModifiedDateInt", date_format(current_time, "yyyyMMddHHmmssSSS").cast(LongType))
        failTable = correctFormat(failTable, orderOfFailTable)
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RUNDETAILS_TABLE_NAME") + " " + dimaggExpression)
        val failedViewName = "vw_Failed_" + view_uid
        failTable.createOrReplaceTempView(failedViewName)
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_FAIL_TABLE_NAME") + " select * from " + failedViewName)
      }
      else
        println("Skipping result logging as DQ_LOG_RESULTS_FLAG is set to " + properties.getProperty("DQ_LOG_RESULTS_FLAG"))

      dqResultDf
    }
    catch{
      case e: Exception=> println("Return original dataframe. Nullcheck for source->"+sourcename+" and entity->"+entityname+" failed with exception->\n"+e.toString)
      df
    }
  }

  // Method to invoke null check on a dataset based on arguments passed by the user
  // Returns the orginial dataframe along with additional flags indicating whether the required columns have passed/failed the null check for each particular record
  def apply(df:Dataset[Row],params:String,colEntitySourceMap:Map[String,List[String]],originalDfColumns:Seq[String],spark:SparkSession,pipelineId: String, properties: Properties): Dataset[Row]= {
    try {
      //applying sanity checks to check the ruleName as "nullcheck"
      val ruleName = "nullcheck"
      if (params == null) {
        println("Skipping " + ruleName + ". Please send Parameters json as string for "+ruleName)
        return df
      }
      import spark.implicits._
      //getting the required paramaeters from the JSON:params to apply nullcheck (eg , columnList)
      val paramsSchema = List(
        StructField("params", StringType, false))
      val paramsRow = Seq(Row(params))
      val paramsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(paramsRow),
        StructType(paramsSchema)
      )
      val paramsString = paramsDf.select(col("params") as "params").map(_.toString())
      val readJson = spark.read.json(paramsString).asInstanceOf[Dataset[Row]]
      val readJsonCols = readJson.columns.toSeq

      //sanity check to validate json:params
      if (containsIgnoreCase(readJsonCols, "_corrupt_record")) {
        println("Skipping " + ruleName + ". The Parameters for "+ruleName+" are not a valid Json. Please provide a valid Json")
        return df
      }
      //sanity check for required column:columnList in params
      if (!containsIgnoreCase(readJsonCols, "columnList")) {
        println("Skipping " + ruleName + ". Mandatory Key \"columnList\" required in Parameters for nullcheck")
        return df
      }
      //Getting list of columns:distinctColList to apply nullcheck on
      val columns = readJson.select("columnList").first.getString(0)
      val colList = columns.split(",").toList
      val distinctColList = colList.distinct

      //view name for the final result
      val view_uid = java.util.UUID.randomUUID.toString.replace('-', '_')
      val resultViewName = "vw_Result_" + view_uid

      ////columns of the dqfailtable
      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")
      //sql expression for applying nullcheck will come here
      var sqlexpression = ""
      //sql expression for failed nullcheck will come here
      var sqllogExpression = ""
      //sql expression for logging relevant information will come here
      var dimaggExpression = ""
      val recordCount = df.count()

      val colListAsString = getStringFromSeq(originalDfColumns)
      //traversing through all columns to apply null check
      for (colName <- distinctColList) {
        if (!containsIgnoreCase(originalDfColumns, colName)) {
          println("Skipping " + ruleName + " for column " + colName + " as it does not exist in frame provided")
        }
        else {
          val dqAggKey = java.util.UUID.randomUUID.toString
          val entityname = colEntitySourceMap(colName)(0)
          val sourcename = colEntitySourceMap(colName)(1)
          sqlexpression = sqlexpression + ",case when " + colName + " is null then false else true end as " + colName + "_" + ruleName
          sqllogExpression = sqllogExpression + (if (sqllogExpression != "") " union all " else "") + "Select  to_json(struct(" + colListAsString + "))as Record,'" + sourcename + "' as Source,'" + entityname + "' as Entity,'" + colName + "' as ColumnName,'" + dqAggKey + "' as DqAggTableKey  from " + resultViewName + " where " + colName + "_" + ruleName + "=false"
          dimaggExpression += (if (dimaggExpression != "") " union all " else "") + "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + colName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId ,date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        }
      }

      //sanity check if the null check expression was built successfully
      if (sqlexpression == "" || sqllogExpression == "" || dimaggExpression == "") {
        return df
      }

      val inputViewName = "vw_Input_" + view_uid
      df.createOrReplaceTempView(inputViewName)
      //applying null check
      val dqResultDf = spark.sql("select *" + sqlexpression + " from " + inputViewName)

      dqResultDf.createOrReplaceTempView(resultViewName)

      if (properties.getProperty("DQ_LOG_RESULTS_FLAG").toBoolean) {
        //logging results in required tables
        val failedresult = spark.sql(sqllogExpression)
        val current_time = current_timestamp()
        var failTable = failedresult.withColumn("Rule", lit(ruleName)).withColumn("PlatformModifiedDate", date_format(current_time, "y-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("PlatformModifiedDateInt", date_format(current_time, "yyyyMMddHHmmssSSS").cast(LongType))
        failTable = correctFormat(failTable, orderOfFailTable)
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RUNDETAILS_TABLE_NAME") + " " + dimaggExpression)
        val failedViewName = "vw_Failed_" + view_uid
        failTable.createOrReplaceTempView(failedViewName)
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_FAIL_TABLE_NAME") + " select * from " + failedViewName)
      }
      else
        println("Skipping result logging as DQ_LOG_RESULTS_FLAG is set to " + properties.getProperty("DQ_LOG_RESULTS_FLAG"))

      dqResultDf
    }
    catch {
      case e: Exception => println("Returning original Dataframe. Nullcheck failed with Exception-->\n" + e.toString)
        df
    }
  }
}