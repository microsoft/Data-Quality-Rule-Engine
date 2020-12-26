// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.ms.dq.rules

import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.ms.dq.support.SupportTrait
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType};

class Schemacheck extends SupportTrait{
  def check(df:Dataset[Row],ruleName:String,schema:StructType,bodyColumn:String,spark:SparkSession):Dataset[Row]={
    import spark.implicits._
    val dfCount=df.count()
    val resultColumn=bodyColumn+"_"+ruleName
    val recordDfCols=Seq(bodyColumn)
    val dfStringDS=df.select(col(bodyColumn)as bodyColumn).map(_.toString())
    val basePath="DqSchemacheck"
    val uuid=java.util.UUID.randomUUID.toString

    // path to store records which are not inline with the schema
    val baseDqFolder="/tmp/"+basePath+"/"+uuid

    //imposing schema. Records which are inline with the schema are stored in validRecordsTemp , the others are stored in the baseDqFolder path
    val validRecordsTemp=spark.read.option("badRecordsPath", baseDqFolder).schema(schema).json(dfStringDS)

    //reconverting records back to jsons
    val validRecords=validRecordsTemp.toJSON.asInstanceOf[Dataset[Row]].toDF(recordDfCols: _*).withColumn(resultColumn,lit(true))
    var resultDf=validRecords
    val validRecordsCount=validRecords.count()

    //checking if there were any dq failures
    if(validRecordsCount != dfCount){
      //getting all failed records and adding it to the resulting dataframe
      val colNameTimestamp="name"
      val badRecordsName="bad_records"
      val recordName="record"
      val fileNameList=dbutils.fs.ls(baseDqFolder)
      val fileNameWithTS=fileNameList(0).path
      val fileName=fileNameWithTS+"/"+badRecordsName
      val badRecordsTemp=spark.read.json(fileName).select(recordName)
      val badRecords=badRecordsTemp.withColumn(recordName,col(recordName).substr(lit(2),length(col(recordName))-2)).toDF(recordDfCols: _*).withColumn(resultColumn,lit(false))
      resultDf=validRecords.union(badRecords)
    }
    resultDf
  }

  // Method to invoke schema check on a dataset:df based on metadata
  // Returns the original dataframe along with additional flags indicating whether the required columns have passed/failed the schema check for each particular record
  // Schema checks the schema of the bodyColumn whose values are expected to be Jsons
  def apply(df:Dataset[Row],schema:StructType,bodyColumn:String,entityname:String,sourcename:String,spark:SparkSession, pipelineId: String, properties: Properties):Dataset[Row]= {
    try {
      import spark.implicits._
      val ruleName = "schemacheck"
      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")
      val recordDfCols = Seq("Record")
      val dqResultDf = check(df, ruleName, schema, bodyColumn, spark)

      if (properties.getProperty("DQ_LOG_RESULTS_FLAG").toBoolean) {
        //logging results in required tables
        val failed = dqResultDf.filter(col(bodyColumn + "_" + ruleName) === false).select(bodyColumn)
        val recordDf = failed.toDF(recordDfCols: _*)
        val dqAggKey = java.util.UUID.randomUUID.toString
        val current_time = current_timestamp()

        var failTable = recordDf.withColumn("Source", lit(sourcename)).withColumn("Entity", lit(entityname)).withColumn("ColumnName", lit(null)).withColumn("Rule", lit(ruleName)).withColumn("PlatformModifiedDate", date_format(current_time, "y-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("PlatformModifiedDateInt", date_format(current_time, "yyyyMMddHHmmssSSS").cast(LongType)).withColumn("DqAggTableKey", lit(dqAggKey))
        failTable = correctFormat(failTable, orderOfFailTable)

        val recordCount = df.count()
        val dimAggExpression = "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId , date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RUNDETAILS_TABLE_NAME") + " " + dimAggExpression)

        val failedViewName = "vw_Failed_" + dqAggKey.replace('-', '_')
        failTable.createOrReplaceTempView(failedViewName)
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_FAIL_TABLE_NAME") + " select * from " + failedViewName)
      }
      else
        println("Skipping result logging as DQ_LOG_RESULTS_FLAG is set to " + properties.getProperty("DQ_LOG_RESULTS_FLAG"))

      dqResultDf
    }
    catch {
      case e: Exception=> println("Returning original Dataframe. Schemacheck for Source->"+sourcename+" and entity->"+entityname+" failed with Exception-->\n"+e.toString)
        df
    }
  }
}