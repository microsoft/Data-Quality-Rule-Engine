// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.ms.dq.rules

import java.util.Properties

import com.ms.dq.support.SupportTrait
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class Uniquecheck extends SupportTrait {
  // Method to invoke unique check on a dataset:df based on metadata entry
  // Returns the orginial dataframe along with additional flags indicating whether the required columns have passed/failed the unique check for each particular record
  def apply(df: Dataset[Row], params: String, entityname: String, sourcename: String, spark: SparkSession, pipelineId: String, properties: Properties): Dataset[Row]= {
    try {
      val ruleName = "uniquecheck"
      if (params == null) {
        println("Skipping " + ruleName + ". Please input Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + " in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //getting the required paramaeters from the JSON:params to apply nullcheck (eg , columnList)
      import spark.implicits._
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
        println("Skipping " + ruleName + ". The Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + " are not a valid Json. Please input a valid Json in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //Getting list of columns:distinctColList to apply unique check on
      if (!containsIgnoreCase(readJsonCols, "columnList")) {
        println("Skipping " + ruleName + ". Mandatory Key \"columnList\" required in Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + ". Please make the required changes in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //get the number of unique checks to be performed on column (be it single or composite)
      var len: Integer  =readJson.select(size($"columnList")).first.getInt(0).toInt
      var i= 0
      var listOfCompositeColumns = List[String]()
      for( i <- 0 to len-1)
      {
        val colm = readJson.select($"columnList".getItem(i)).first.getString(0)
        listOfCompositeColumns = listOfCompositeColumns :+ colm
      }
      val distinctColList = listOfCompositeColumns

      var latestIdentifierCol: String = null
      if (containsIgnoreCase(readJsonCols, "latestrowidentifier")) {
        latestIdentifierCol = readJson.select("latestrowidentifier").first.getString(0)
      }

      var originalDfColumns = df.columns.toSeq
      if (containsIgnoreCase(originalDfColumns, "dq_uniqueID")) {
        originalDfColumns = originalDfColumns.filter(!_.contains("dq_uniqueID"))
      }

      if (latestIdentifierCol != null && !containsIgnoreCase(originalDfColumns, latestIdentifierCol)) {
        println("Value for latestrowidentifier=" + latestIdentifierCol + " in parameters for " + ruleName + " source=" + sourcename + " entity=" + entityname + " is not present in the given dataframe. Please make the required change. Proceeding with " + ruleName + " without identifying latest column.")
        latestIdentifierCol = null
      }
      //columns of the dqfailtable in particular order
      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")

      //sql expression for uniquecheck will come here
      var sqlexpression = ""
      //sql expression for building the failed records for uniquecheck will come here
      var sqllogExpression = ""
      //sql expression for logging details of the DQ rule check will come here
      var dimaggExpression = ""
      val recordCount = df.count()

      //view name for the final result
      val view_uid = java.util.UUID.randomUUID.toString.replace('-', '_')
      val resultViewName = "vw_Result_" + view_uid

      val colListAsString = getStringFromSeq(originalDfColumns)
      //traversing the columnlist(single/composite) on which uniquecheck is to applied
      //in this loop we build the sql expression for uniquecheck
      for (colName <- distinctColList) {

        if (!containsIgnoreCase(originalDfColumns, colName.split(","))) {
          println("Skipping " + ruleName + " for column " + colName + " as it does not exist in the frame provided. Please check parameters in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME") + " for " + ruleName + " source=" + sourcename + " and entity=" + entityname)
        }
        else {
          //building the column name which  will reflect the status of uniquecheck for individual records
          //colName = "id,createdDate" will be changed to uniquecheck_id_createdDate which will reflect the status of uniquecheck of a particular recor based on the mentioned columns
          val colName_as_col = colName.replaceAll(",", "_")
          sqlexpression += ",case when count(*) over (partition by " + colName + ") > 1 then false else true end as " + colName_as_col + "_" + ruleName //replace with function call
          if (latestIdentifierCol != null) {
            sqlexpression += ",case when row_number() over(partition by " + colName + " order by " + latestIdentifierCol + " desc)=1 then true else false end as LatestRow_" + colName_as_col
          }
          val dqAggKey = java.util.UUID.randomUUID.toString
          sqllogExpression = sqllogExpression + (if (sqllogExpression != "") " union all " else "") + "Select  to_json(struct(" + colListAsString + "))as Record,'" + sourcename + "' as Source,'" + entityname + "' as Entity,'" + colName + "' as ColumnName,'" + dqAggKey + "' as DqAggTableKey  from " + resultViewName + " where " + colName_as_col + "_" + ruleName + "= false"
          dimaggExpression += (if (dimaggExpression != "") " union all " else "") + "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + colName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId ,  date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        }
      }

      if (sqlexpression == "" || sqllogExpression == "" || dimaggExpression == "") {
        return df
      }

      //creating resulting dataframe with required DQ columns
      val inputViewName = "vw_Input_" + view_uid
      df.createOrReplaceTempView(inputViewName)
      val dqResultDf = spark.sql("select *" + sqlexpression + " from " + inputViewName)
      dqResultDf.createOrReplaceTempView(resultViewName)

      if (properties.getProperty("DQ_LOG_RESULTS_FLAG").toBoolean) {
        //log results into required tables
        val failedresult = spark.sql(sqllogExpression)

        val current_time = current_timestamp()
        var failTable = failedresult.withColumn("Source", lit(sourcename)).withColumn("Entity", lit(entityname)).withColumn("Rule", lit(ruleName)).withColumn("PlatformModifiedDate", date_format(current_time, "y-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("PlatformModifiedDateInt", date_format(current_time, "yyyyMMddHHmmssSSS").cast(LongType))
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
      case e: Exception=> println("Returning original Dataframe. Uniquecheck for Source->"+sourcename+" and entity->"+entityname+" failed with Exception-->\n"+e.toString)
        df
    }
  }
  // Method to invoke null check on a dataset based on arguments passed by the user
  // Returns the orginial dataframe along with additional flags indicating whether the required columns have passed/failed the unique check for each particular record
  def apply(df:Dataset[Row],params:String,colEntitySourceMap:Map[String,List[String]],originalDfColumns:Seq[String],spark:SparkSession,pipelineId: String,properties: Properties):Dataset[Row]={
    try {
      //applying sanity checks to check the ruleName as "uniquecheck"
      val ruleName = "uniquecheck"
      if (params == null) {
        println("Skipping " + ruleName + ". Please send Parameters json as string for " + ruleName)
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
        println("Skipping " + ruleName + ". The Parameters for " + ruleName + " are not a valid Json. Please provide a valid Json")
        return df
      }
      //sanity check for required column:columnList in params
      if (!containsIgnoreCase(readJsonCols, "columnList")) {
        println("Skipping " + ruleName + ". Mandatory Key \"columnList\" required in Parameters for nullcheck")
        return df
      }
      //get the number of unique checks to be performed on column (be it single or composite)
      var len: Integer = readJson.select(size($"columnList")).first.getInt(0).toInt
      var i = 0
      var listOfCompositeColumns = List[String]()
      //traversing each of the columnList provided(single/composite) and storing it in list
      for (i <- 0 to len - 1) {
        val colm = readJson.select($"columnList".getItem(i)).first.getString(0)
        listOfCompositeColumns = listOfCompositeColumns :+ colm
      }
      val distinctColList = listOfCompositeColumns

      var latestIdentifierCol: String = null
      if (containsIgnoreCase(readJsonCols, "latestrowidentifier")) {
        latestIdentifierCol = readJson.select("latestrowidentifier").first.getString(0)
      }

      if (latestIdentifierCol != null && !containsIgnoreCase(originalDfColumns, latestIdentifierCol)) {
        println("Value for latestrowidentifier=" + latestIdentifierCol + " in parameters for " + ruleName + "  is not present in the given dataframe. Please make the required change. Proceeding with " + ruleName + " without identifying latest column.")
        latestIdentifierCol = null
      }
      //order of the attributes of the failed Table
      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")
      val colListAsString = getStringFromSeq(originalDfColumns)
      //sql expression for building failed records for uniquecheck will come here
      var sqllogExpression = ""
      //sql expression for uniquecheck will come here
      var sqlexpression = ""
      //sql expression for logging the DQ run will come here
      var dimaggExpression = ""
      val recordCount = df.count()

      //
      val view_uid = java.util.UUID.randomUUID.toString.replace('-', '_')
      val resultViewName = "vw_Result_" + view_uid
      // traversing each of the columnlist and applying uniquechek
      for (colName <- distinctColList) {
        if (!containsIgnoreCase(originalDfColumns, colName.split(","))) {
          println("Skipping " + ruleName + " for column " + colName + " as it does not exist in frame provided")
        }
        else {
          //building the column name which  will reflect the status of uniquecheck for individual records
          //colName = "id,createdDate" will be changed to uniquecheck_id_createdDate which will reflect the status of uniquecheck of a particular recor based on the mentioned columns
          val colName_as_col = colName.replaceAll(",", "_")
          val entityname = colEntitySourceMap(colName)(0)
          val sourcename = colEntitySourceMap(colName)(1)
          sqlexpression += ",case when count(*) over (partition by " + colName + ") > 1 then false else true end as " + colName_as_col + "_" + ruleName //replace with function call
          if (latestIdentifierCol != null) {
            sqlexpression += ",case when row_number() over(partition by " + colName + " order by " + latestIdentifierCol + " desc)=1 then true else false end as LatestRow_" + colName_as_col
          }
          val dqAggKey = java.util.UUID.randomUUID.toString
          sqllogExpression = sqllogExpression + (if (sqllogExpression != "") " union all " else "") + "Select  to_json(struct(" + colListAsString + "))as Record,'" + sourcename + "' as Source,'" + entityname + "' as Entity,'" + colName + "' as ColumnName,'" + dqAggKey + "' as DqAggTableKey  from " + resultViewName + " where " + colName_as_col + "_" + ruleName + "= false"
          dimaggExpression += (if (dimaggExpression != "") " union all " else "") + "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + colName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId, date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        }
      }

      //sanity check if the unique check expression was built successfully
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
    catch {
      case e: Exception=> println("Returning original Dataframe. Uniquecheck failed with Exception-->\n"+e.toString)
        df
    }
  }
}
