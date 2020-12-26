// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Databricks notebook source
// DBTITLE 1,Input Widgets
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

dbutils.widgets.text("keyAdls", "","keyAdls")
dbutils.widgets.text("credendentialAdls", "","credendentialAdls")
dbutils.widgets.text("databricksScope", "","databricksScope")
dbutils.widgets.text("adlsLoginUrl", "","adlsLoginUrl")
dbutils.widgets.text("datalakeName", "","datalakeName")
dbutils.widgets.text("adlsPath", "","adlsPath")
dbutils.widgets.text("subjectAreaDb", "","subjectAreaDb")
dbutils.widgets.text("dqRunDetailsTableName", "","dqRunDetailsTableName")
dbutils.widgets.text("dqFailTableName", "","dqFailTableName")
dbutils.widgets.text("dqAggTableName", "","dqAggTableName")
dbutils.widgets.text("dqWatermarkTableName", "","dqWatermarkTableName")


// COMMAND ----------

// DBTITLE 1,ADLS Gen2 Access Config
val keyAdls = dbutils.widgets.get("keyAdls")
val credendentialAdls = dbutils.widgets.get("credendentialAdls")
val databricksScope = dbutils.widgets.get("databricksScope")

val decryptedADLSId = dbutils.secrets.get(scope = databricksScope, key = keyAdls)
val decryptedADLSCredential = dbutils.secrets.get(scope = databricksScope, key = credendentialAdls)
val adlsLoginUrl = dbutils.widgets.get("adlsLoginUrl")
val datalakeName = dbutils.widgets.get("datalakeName")

//initializing the spark session with the config

spark.conf.set(s"fs.azure.account.auth.type.${datalakeName}.dfs.core.windows.net", "OAuth")
spark.conf.set(s"fs.azure.account.oauth.provider.type.${datalakeName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(s"fs.azure.account.oauth2.client.id.${datalakeName}.dfs.core.windows.net", s"${decryptedADLSId}")
spark.conf.set(s"fs.azure.account.oauth2.client.secret.${datalakeName}.dfs.core.windows.net", s"${decryptedADLSCredential}")
spark.conf.set(s"fs.azure.account.oauth2.client.endpoint.${datalakeName}.dfs.core.windows.net", adlsLoginUrl)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")


// COMMAND ----------

// DBTITLE 1,Initialize Variables
val subjectAreaDb = dbutils.widgets.get("subjectAreaDb")
val dqRunDetailsTableName = dbutils.widgets.get("dqRunDetailsTableName")
val dqFailTableName = dbutils.widgets.get("dqFailTableName")
val dqAggTableName = dbutils.widgets.get("dqAggTableName")
val dqWatermarkTableName = dbutils.widgets.get("dqWatermarkTableName")
val adlsPath = dbutils.widgets.get("adlsPath")

// COMMAND ----------

// DBTITLE 1,Common Methods
import org.apache.spark.sql.DataFrame
def getwatermarkvalue(dqtablename: String): String ={

  try{
    val watermarkendvalue = spark.sql("SELECT COALESCE(watermarkendvalue, '1753-01-01') as watermarkvalue FROM " + subjectAreaDb + "." + dqWatermarkTableName + " WHERE lower(dqtable)='" + dqtablename +"'")

    if(watermarkendvalue.head(1).isEmpty)
      return "1753-01-01"
    else
      return watermarkendvalue.first.getString(0)
  }

  catch {
    case e: Exception => {
      println("ERROR : Unable to get the WaterMark value " + e.getMessage)
      throw e
    }
  }

}


def getwatermarkvalue(dqtablename: String, source: String, entity: String): String ={

  try{
    val watermarkendvalue = spark.sql("SELECT COALESCE(watermarkendvalue, '1753-01-01') as watermarkvalue FROM " + subjectAreaDb + "." + dqWatermarkTableName + " WHERE lower(dqtable)='" + dqtablename +"' and lower(source)='" + source +"' and lower(entity)='" + entity +"'")

    if(watermarkendvalue.head(1).isEmpty)
      return "1753-01-01"
    else
      return watermarkendvalue.first.getString(0)
  }

  catch {
    case e: Exception => {
      println("ERROR : Unable to get the WaterMark value " + e.getMessage)
      throw e
    }
  }

}


def setwatermarkvalue(watermarkendvalue: String, dqtablename: String, source: String, entity: String): Unit ={

  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  val insertQuery = s""" MERGE INTO ${subjectAreaDb}.${dqWatermarkTableName} as Target
      USING   (
      SELECT   '${watermarkendvalue}'   AS watermarkendvalue
      ,current_timestamp        AS omidqcreateddate
      ,current_timestamp        AS omidqmodifieddate
      ,'${dqtablename}'         AS dqtable
      ,'${source}'  			AS source
      ,'${entity}'  			AS entity
      ) AS Source
      ON     Target.dqtable ='${dqtablename}'
      AND COALESCE(Target.source, '') = COALESCE(Source.source, '')
      AND COALESCE(Target.entity, '') = COALESCE(Source.entity, '')
      WHEN   MATCHED
      AND COALESCE(Target.watermarkendvalue, '') <> COALESCE(Source.watermarkendvalue, '')
      AND COALESCE(Target.omidqmodifieddate, '') <> COALESCE(Source.omidqmodifieddate, '')
      THEN
      UPDATE
      SET   Target.watermarkendvalue  = Source.watermarkendvalue
      ,Target.omidqmodifieddate = Source.omidqmodifieddate
      WHEN   NOT MATCHED
      THEN
      INSERT * """

  try {
    spark.sql(insertQuery)
  }
  catch {
    case e: Exception => {
      println("ERROR : Unable to insert the WaterMark value " + e.getMessage)
      throw e
    }
  }

}

// COMMAND ----------

// DBTITLE 1,Populate dqAggTable
val watermarkvalue_dqfailtable = getwatermarkvalue(dqAggTableName,"dq",dqFailTableName)
println(s"Watermark value for ${dqAggTableName}:${dqFailTableName} is: "+watermarkvalue_dqfailtable)
val watermarkvalue_dqrundetails = getwatermarkvalue(dqAggTableName,"dq",dqRunDetailsTableName)
println(s"Watermark value for ${dqAggTableName}:${dqRunDetailsTableName} is: "+watermarkvalue_dqrundetails)

val watermarkendvalue_dqfailtable = spark.sql("SELECT COALESCE(MAX(PlatformModifieddate), '1753-01-01') FROM " + subjectAreaDb + "." + dqFailTableName)
println(s"New Watermark value for ${dqAggTableName}:${dqFailTableName} is: "+watermarkendvalue_dqfailtable.first.get(0).toString())
val watermarkendvalue_dqrundetails = spark.sql("SELECT COALESCE(MAX(PlatformModifieddate), '1753-01-01') FROM " + subjectAreaDb + "." + dqRunDetailsTableName)
println(s"New Watermark value for ${dqAggTableName}:${dqRunDetailsTableName} is: "+watermarkendvalue_dqrundetails.first.get(0).toString())

if (watermarkendvalue_dqfailtable.first.get(0).toString() == watermarkvalue_dqfailtable && watermarkendvalue_dqrundetails.first.get(0).toString() == watermarkvalue_dqrundetails)
{
  println("No records to update")
}
else
{
  println(s"New Records detected. Existing Watermark for ${dqRunDetailsTableName}: " + watermarkvalue_dqrundetails + s" and for ${dqFailTableName}: "+watermarkvalue_dqfailtable)

  val mergeQuery = s""" MERGE
          INTO   ${subjectAreaDb}.${dqAggTableName} as Target
          USING   (
          SELECT R.DqAggTableKey                    AS DqAggTableKey
          ,R.Source                                 AS Source
          ,R.Entity                                 AS Entity
          ,R.ColumnName                             AS ColumnName
          ,R.Rule                                   AS Rule
          ,R.PlatformModifiedDate                   AS DQModifiedDate
          ,R.PlatformModifiedDateInt                AS DQModifiedDateInt
          ,R.RecordCount                            AS RecordCount
          ,COALESCE(F.FailCount, 0)                 AS FailCount
          ,R.RecordCount - COALESCE(F.FailCount, 0) AS SuccessCount
          ,current_timestamp                        AS PlatformModifiedDate
          ,CAST(date_format(current_date(), 'yyyyMMddhhmmssSSS') AS Long)     AS PlatformModifiedDateInt
          FROM ${subjectAreaDb}.${dqRunDetailsTableName} R
          LEFT JOIN (
          SELECT  DqAggTableKey
          ,Count(1) AS FailCount
          ,MIN(PlatformModifiedDate) AS FailTablePlatformModifiedDate
          FROM ${subjectAreaDb}.${dqFailTableName}
          GROUP BY DqAggTableKey) F
            ON R.DqAggTableKey = F.DqAggTableKey
          WHERE R.PlatformModifiedDate >= '${watermarkvalue_dqrundetails}'
          OR F.FailTablePlatformModifiedDate >= '${watermarkvalue_dqfailtable}'
          ) AS Source
          ON     Target.DqAggTableKey = Source.DqAggTableKey
          WHEN   MATCHED THEN
          UPDATE
          SET   Target.Source             = Source.Source
          ,Target.Entity                  = Source.Entity
          ,Target.Rule                    = Source.Rule
          ,Target.ColumnName              = Source.ColumnName
          ,Target.FailCount		          = Source.FailCount
          ,Target.DQModifiedDate	      = Source.DQModifiedDate
          ,Target.DQModifiedDateInt       = Source.DQModifiedDateInt
          ,Target.RecordCount		      = Source.RecordCount
          ,Target.SuccessCount	          = Source.SuccessCount
          ,Target.PlatformModifiedDate    = Source.PlatformModifiedDate
          ,Target.PlatformModifiedDateInt = Source.PlatformModifiedDateInt
          WHEN   NOT MATCHED
          THEN
          INSERT * """

  try {
    spark.sql(mergeQuery)
  }
  catch {
    case e: Exception => {
      println("ERROR : Unable to merge data in dqAggTable " + e.getMessage)
      throw e
    }
  }

  println(s"Setting Watermark for ${dqAggTableName}:${dqFailTableName}. Value: "+ watermarkendvalue_dqfailtable.first.get(0).toString())
  setwatermarkvalue(watermarkendvalue_dqfailtable.first.get(0).toString(), dqAggTableName, "dq", dqFailTableName)
  println(s"Setting Watermark for ${dqAggTableName}:${dqRunDetailsTableName}. Value: "+ watermarkendvalue_dqrundetails.first.get(0).toString())
  setwatermarkvalue(watermarkendvalue_dqrundetails.first.get(0).toString(), dqAggTableName , "dq", dqRunDetailsTableName )
  println("Watermark set")
}

// COMMAND ----------

// DBTITLE 1,Write files by Date to cooked folder for dqFailTable
import org.apache.spark.sql.functions._
import scala.collection.parallel._
val df = spark.table(s"${subjectAreaDb}.${dqFailTableName}")
val sources = df.select(lower($"Source")).distinct.collect.toList

val watermarkvalue = getwatermarkvalue(dqFailTableName)
println("Watermark value is: "+watermarkvalue)
val watermarkendvalue = spark.sql("SELECT COALESCE(MAX(PlatformModifieddate), '1753-01-01') FROM " + subjectAreaDb + "." + dqFailTableName)
println("New Watermark value is: "+watermarkendvalue.first.get(0).toString())

if (watermarkendvalue.first.get(0).toString() == watermarkvalue)
{
  println("No records to update")
}
else
{
  println("New Records detected. Watermark: "+watermarkvalue)
  val dates = df.filter(to_date($"PlatformModifieddate") >= watermarkvalue).select(to_date($"PlatformModifieddate") as "date").distinct
  //display(dates)

  for(d <- dates.collect)
  {
    println("Writing File for Date: " + d(0))
    val filtereddf = df.filter(to_date($"PlatformModifiedDate") === d(0))

    filtereddf.repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("sep", "\t")
      .option("quoteAll", true)
      .option("escape","\"")
      .mode("overwrite")
      .save(adlsPath+"/" + dqFailTableName + "/" +d(0)+ ".tmp")

    val partitionPath = dbutils.fs.ls(adlsPath+"/" + dqFailTableName + "/" +d(0)+ ".tmp").filter(file => file.name.endsWith("csv"))(0).path
    dbutils.fs.cp(partitionPath, adlsPath+"/" + dqFailTableName + "/"  +d(0)+ ".tsv")
    dbutils.fs.rm(adlsPath+"/" + dqFailTableName + "/" +d(0)+ ".tmp", recurse = true)

    println("Completed writing File for Date: " + d(0))

  }
}

println("Setting Watermark. Value: "+ watermarkendvalue.first.get(0).toString())
setwatermarkvalue(watermarkendvalue.first.get(0).toString(), dqFailTableName , null, null)
println("Watermark set")





// COMMAND ----------

// DBTITLE 1,Write dqaggtable to cooked folder
val df = spark.table(s"${subjectAreaDb}.${dqAggTableName}")
df.repartition(1).write
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("quoteAll", true)
  .option("escape","\"")
  .mode("overwrite")
  .save(adlsPath+"/" + dqAggTableName + ".tmp")

val partitionPath = dbutils.fs.ls(adlsPath+"/" + dqAggTableName + ".tmp/").filter(file => file.name.endsWith("csv"))(0).path
dbutils.fs.cp(partitionPath, adlsPath+"/" + dqAggTableName + ".tsv")
dbutils.fs.rm(adlsPath+"/" + dqAggTableName +  ".tmp/", recurse = true)


// COMMAND ----------

// DBTITLE 1,Write dqrundetails to cooked folder
val df = spark.table(s"${subjectAreaDb}.${dqRunDetailsTableName}")
df.repartition(1).write
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("quoteAll", true)
  .option("escape","\"")
  .mode("overwrite")
  .save(adlsPath+"/" + dqRunDetailsTableName + ".tmp")

val partitionPath = dbutils.fs.ls(adlsPath+"/" + dqRunDetailsTableName + ".tmp/").filter(file => file.name.endsWith("csv"))(0).path
dbutils.fs.cp(partitionPath, adlsPath+"/" + dqRunDetailsTableName + ".tsv")
dbutils.fs.rm(adlsPath+"/" + dqRunDetailsTableName + ".tmp/", recurse = true)

// COMMAND ----------


