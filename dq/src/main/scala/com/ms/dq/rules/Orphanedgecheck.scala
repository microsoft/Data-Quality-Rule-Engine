// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Checks whether the dataframe’s values referring to ids of its parent dataframes, actually exist in the parent dataframes.
package com.ms.dq.rules

import java.util.{Calendar, Properties}

import com.ms.dq.support.SupportTrait
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
class Orphanedgecheck extends SupportTrait{
  // Method to invoke orphan edge check on a dataset:df based on metadata
  // Returns the orginial dataframe along with additional flags indicating whether the required columns have passed/failed the orphan edge check for each particular record
  def apply(df:Dataset[Row],params:String,entityname:String,sourcename:String,spark:SparkSession, pipelineId: String, properties: Properties):Dataset[Row]= {
    try {
      val ruleName = "orphanedgecheck"
      if (params == null) {
        println("Skipping " + ruleName + ". Please input Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + " in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      //getting the required paramaeters from the JSON:params to apply orphanedge check (eg , tableName)
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
      //sanity check for required keys in json:params
      if (!containsIgnoreCase(readJsonCols, "tableName")) {
        println("Skipping " + ruleName + ". Mandatory Key \"tableName\" required in Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + ". Please make the required changes in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      val tableName = readJson.select("tableName").first.getString(0)
      if (!containsIgnoreCase(readJsonCols, "auditColIntName")) {
        println("Skipping " + ruleName + ". Mandatory Key \"auditColIntName\" required in Parameters for " + ruleName + " on source=" + sourcename + " and entity=" + entityname + ". Please make the required changes in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME"))
        return df
      }
      val auditCol = readJson.select("auditColIntName").first.getString(0)

      //obtaining optional keys from json:params
      var hours:Long=0
      var minutes:Long=0
      var seconds:Long=0
      if(containsIgnoreCase(readJsonCols,"cutOffHours"))
      {
        try {
          hours=readJson.select("cutOffHours").first.getLong(0)
        }
        catch {
          case e: Exception=>println("Error in metadata of source->"+sourcename+" entity->"+entityname+" rule->"+ruleName+" parameter->cutOffHours(must be long)")
            throw e
        }
      }
      if(containsIgnoreCase(readJsonCols,"cutOffMinutes"))
      {
        try {
          minutes = readJson.select("cutOffMinutes").first.getLong(0)
        }
        catch {
          case e: Exception=>println("Error in metadata of source->"+sourcename+" entity->"+entityname+" rule->"+ruleName+" parameter->cutOffMinutes(must be long)")
            throw e
        }
      }
      if(containsIgnoreCase(readJsonCols,"cutOffSeconds"))
      {
        try {
          seconds = readJson.select("cutOffSeconds").first.getLong(0)
        }
        catch {
          case e: Exception=>println("Error in metadata of source->"+sourcename+" entity->"+entityname+" rule->"+ruleName+" parameter->cutOffSeconds(must be long)")
            throw e
        }
      }

      //According to the parameters , computing the cutoff time
      var cutOffEndVal=Long.MaxValue.toString()
      if(!(hours==0 && minutes ==0 && seconds == 0))
        {
          val current_ts=spark.sql("select current_timestamp() as current_timestamp").withColumn("SLA",col("current_timestamp") - expr("INTERVAL "+hours+" HOURS")).withColumn("SLA",col("SLA") - expr("INTERVAL "+minutes+" minutes")).withColumn("SLA",col("SLA") - expr("INTERVAL "+seconds+" seconds"))
          cutOffEndVal=current_ts.withColumn("CutOffDate", date_format(col("SLA"), "yyyyMMddHHmmssSSS").cast(LongType)).select("CutOffDate").head().getLong(0).toString()
        }

      //getting delta records based on watermark start and cutoff time
      val waterMarkStart=ruleWaterMarkStart(sourcename,entityname,ruleName,spark,properties)
      val deltaDf=dqDeltaIdentifier(tableName,auditCol,waterMarkStart,cutOffEndVal,spark)
      var dqResultDf: Dataset[Row] = null
      if(deltaDf.isEmpty)
        {
          println("Skipping Orphanedgecheck as no delta data is found. Returning original df ")
          return df
        }

      // getting the original columns of the dataframe (dq_uniqueID is added for internal framework purposes)
      var originalDfColumns = deltaDf.columns.toSeq
      if (containsIgnoreCase(originalDfColumns, "dq_uniqueID")) {
        originalDfColumns = originalDfColumns.filter(!_.contains("dq_uniqueID"))
      }
      import spark.implicits._

      // getting required information from metadata along with sanity checks
      val edgeMetaData = spark.sql("select *,rank()over( partition by source,entity order by fromlookupentity) as fromlookupentityid,rank()over( partition by source,entity order by tolookupentity) as tolookupentityid from " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " where lower(source)='" + sourcename.toLowerCase() + "' and lower(entity)='" + entityname.toLowerCase() + "'")
      val distinctFromId = edgeMetaData.select("fromcolumnname").distinct()
      if (distinctFromId.count() >= 2) {
        println("Skipping orphanedgecheck.Found Multiple fromcolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname + ". Only one should exist.")
        return deltaDf
      }
      val fromIdName = distinctFromId.first.getString(0)
      if (!containsIgnoreCase(originalDfColumns, fromIdName)) {
        println("Skipping " + ruleName + ". " + fromIdName + " not present in provided frame. Please check fromcolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname)
        return deltaDf
      }
      val distincToId = edgeMetaData.select("tocolumnname").distinct()
      if (distincToId.count() >= 2) {
        println("Skipping orphanedgecheck..Found Multiple tocolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname + ". Only one should exist.")
        return deltaDf
      }
      val toIdName = distincToId.first.getString(0)
      if (!containsIgnoreCase(originalDfColumns, toIdName)) {
        println("Skipping " + ruleName + ". " + toIdName + " not present in provided frame. Please check tocolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname)
        return deltaDf
      }

      val orderOfFailTable: List[String] = List("Source", "Entity", "ColumnName", "Rule", "Record", "PlatformModifiedDate", "PlatformModifiedDateInt", "DqAggTableKey")
      var failTable = Seq.empty[(String, String, String)].toDF("Record", "ColumnName", "DqAggTableKey")
      var sqlcasef = ""
      var sqlcaset = ""
      var sqlcaseelsef = ""
      var sqlcaseelset = ""
      var sqljoin = ""
      var frmlst = List[Int]()
      var tolst = List[Int]()

      //applying orphan edge check
      edgeMetaData.select("fromlookupentity", "fromlookupcolumnname", "tolookupentity", "tolookupcolumnname", "filtercondition", "tocolumnname", "fromcolumnname", "fromlookupentityid", "tolookupentityid").sort($"fromlookupentityid", $"tolookupentityid").collect().map(
        {
          d =>
            val fromlookupentity = d.getString(0)
            val colInFromDf = d.getString(1)
            val tolookupentity = d.getString(2)
            val colInToDf = d.getString(3)
            val filtercondition = d.getString(4)
            val tocolumnname = d.getString(5)
            val fromcolumnname = d.getString(6)

            val fromlookupentityid = d.get(7).asInstanceOf[Int].toInt
            val tolookupentityid = d.get(8).asInstanceOf[Int].toInt
            if (!frmlst.contains(fromlookupentityid)) {
              sqljoin = sqljoin + s" \nleft join vwinput${fromlookupentity.replace(".", "_")}   f${fromlookupentityid}  on ${entityname}.${fromcolumnname}=f${fromlookupentityid}.${colInFromDf}"
              if (!spark.catalog.tableExists(fromlookupentity)) {
                println("Skipping OrphanEdgeCheck. No such Table->" + fromlookupentity)
                return deltaDf
              }
              val fromDf = spark.table(fromlookupentity)
              if (!containsIgnoreCase(fromDf.columns.toSeq, colInFromDf)) {
                println("Skipping " + ruleName + ". No column " + colInFromDf + " present in table " + fromlookupentity + ". Please check fromlookupentity and fromlookupcolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname)
                return deltaDf
              }
              fromDf.groupBy(colInFromDf).count().select(colInFromDf).createOrReplaceTempView("vwinput" + fromlookupentity.replace(".", "_"))
              frmlst = frmlst :+ fromlookupentityid
            }
            if (!tolst.contains(tolookupentityid)) {

              sqljoin = sqljoin + s" \nleft join vwinput${tolookupentity.replace(".", "_")}   t${tolookupentityid}  on ${entityname}.${tocolumnname}=t${tolookupentityid}.${colInToDf}"
              if (!spark.catalog.tableExists(tolookupentity)) {
                println("Skipping OrphanEdgeCheck. No such Table->" + fromlookupentity)
                return deltaDf
              }
              val toDf = spark.table(tolookupentity)
              if (!containsIgnoreCase(toDf.columns.toSeq, colInToDf)) {
                println("Skipping " + ruleName + ". No column " + colInToDf + " present in table " + tolookupentity + ". Please check tolookupentity and tolookupcolumnname in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ORPHANEDGEMETADATA_TABLE_NAME") + " for source=" + sourcename + " and entity=" + entityname)
                return deltaDf
              }
              toDf.groupBy(colInToDf).count().select(colInToDf).createOrReplaceTempView("vwinput" + tolookupentity.replace(".", "_"))
              tolst = tolst :+ tolookupentityid
            }
            sqlcasef = sqlcasef + (if (sqlcasef != "") " or " else "") + s"((f${fromlookupentityid}.${colInToDf} is null ) and  ${filtercondition}) "
            sqlcaset = sqlcaset + (if (sqlcaset != "") " or " else "") + s"((t${tolookupentityid}.${colInToDf} is null ) and  ${filtercondition}) "
            sqlcaseelsef = sqlcaseelsef + (if (sqlcaseelsef != "") " or " else "") + s"((f${fromlookupentityid}.${colInToDf} is not null ) and  ${filtercondition}) "
            sqlcaseelset = sqlcaseelset + (if (sqlcaseelset != "") " or " else "") + s"((t${tolookupentityid}.${colInToDf} is not null ) and  ${filtercondition}) "

        })
      val dqAggKey = java.util.UUID.randomUUID.toString
      val inputViewName = "vw_Input_" + dqAggKey.replace('-', '_')
      deltaDf.createOrReplaceTempView(inputViewName)
      var sql = s"\n select ${entityname}.*\n ,case when(${sqlcasef} ) then false when (${sqlcaseelsef}) then true end as ${fromIdName}_${ruleName}\n,case when(${sqlcaset} ) then false when (${sqlcaseelset} ) then true end as ${toIdName}_${ruleName}"
      sql = sql + s"\n from ${inputViewName} ${entityname} \n ${sqljoin}"
      dqResultDf = spark.sql(sql)
      val colListAsString = getStringFromSeq(originalDfColumns)
      val resultViewName = "vw_Result_" + dqAggKey.replace('-', '_')
      dqResultDf.createOrReplaceTempView(resultViewName)

      if (properties.getProperty("DQ_LOG_RESULTS_FLAG").toBoolean) {
        //logging results in required tables
        val toDqAggKey = java.util.UUID.randomUUID.toString
        val failedsql = s" select to_json(struct(${colListAsString})) as Record,'${sourcename}' as Source,'${entityname}' as Entity,'${fromIdName}' as ColumnName,'${dqAggKey}' as DqAggTableKey from ${resultViewName} where ${fromIdName}_${ruleName} =false union all select to_json(struct(${colListAsString}))as Record,'${sourcename}' as Source,'${entityname}' as Entity,'${toIdName}' as ColumnName,'${toDqAggKey}' as DqAggTableKey from ${resultViewName} where ${toIdName}_${ruleName} =false"
        val current_time = current_timestamp()
        failTable = spark.sql(failedsql)
        failTable = failTable.withColumn("Rule", lit(ruleName)).withColumn("PlatformModifiedDate", date_format(current_time, "y-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("PlatformModifiedDateInt", date_format(current_time, "yyyyMMddHHmmssSSS").cast(LongType))
        failTable = correctFormat(failTable, orderOfFailTable)

        val recordCount = deltaDf.count()
        var dimAggExpression = "Select '" + dqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + fromIdName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId, date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        dimAggExpression += " union all "
        dimAggExpression += "Select '" + toDqAggKey + "' as DqAggTableKey, '" + sourcename + "' as Source, '" + entityname + "' as Entity, '" + toIdName + "' as ColumnName, '" + ruleName + "' as Rule, " + recordCount + " as RecordCount, '" + pipelineId + "' as PipelineId, date_format(current_timestamp, \"y-MM-dd'T'HH:mm:ss.SSS'Z'\") as PlatformModifiedDate, cast(date_format(current_timestamp, \"yyyyMMddHHmmssSSS\") as long) as PlatformModifiedDateInt"
        spark.sql("insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_RUNDETAILS_TABLE_NAME") + " " + dimAggExpression)

        val failedViewName = "vw_Failed_" + dqAggKey.replace('-', '_')
        failTable.createOrReplaceTempView(failedViewName)
        spark.sql(s"insert into " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_FAIL_TABLE_NAME") + s" select * from ${failedViewName}")
      }
      else
        println("Skipping result logging as DQ_LOG_RESULTS_FLAG is set to " + properties.getProperty("DQ_LOG_RESULTS_FLAG"))

      val waterMarkEndVal=deltaDf.agg(max(col(auditCol))).head().getLong(0).toString()
      updateRuleWaterMark(sourcename,entityname,ruleName,waterMarkEndVal,spark,properties)

      dqResultDf
    }
    catch {
      case e: Exception=> println("Returning original Dataframe. OrphanEdgeCheck for source->"+sourcename+"and entity->"+entityname+" failed with Exception-->\n"+e.toString)
        df
    }
  }
}