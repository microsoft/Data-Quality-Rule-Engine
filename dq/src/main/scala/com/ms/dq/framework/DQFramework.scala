// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.ms.dq.framework

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import com.ms.dq.rules._

import scala.collection.immutable.Map
import java.util.{Calendar, Date, Properties}

import com.ms.dq.support.SupportTrait

import scala.collection.parallel._
import scala.collection.parallel.immutable.ParMap
import scala.xml.Properties

class DQFramework extends SupportTrait {


  var uniquecheck_latestIdCol: String = null;
  var spark: SparkSession = null;

  // Set spark session for execution instance
  def setSparkSession(_sparkSession: SparkSession) = {
    spark = _sparkSession
  }

  // Method to invoke Data Quality rules on a dataset by reading the metadata table [METADATA BOUND]
  def applyDqInner(df: Dataset[Row], sourceName: String, entityName: String, schema: StructType, pipelineId: String): Dataset[Row] = {
    if (spark == null) {
      println("Please set spark session. Use function setSparkSession(_sparkSession:SparkSession)")
      throw new Exception("Please set spark session. Use function setSparkSession(_sparkSession:SparkSession)")
    }
    if (sourceName == null) {
      println("Source has been sent as null.")
      throw new Exception("Source has been sent as null.")
    }
    if (entityName == null) {
      println("Entity has been sent as null.")
      throw new Exception("Entity has been sent as null.")
    }

    // Read the application.properties
    val properties = getProperties()

    // intialise the dataframe to be returned , so that all the existing columns are present
    var return_Df = df
    if (schema != null) {
      //retrieves the schema defined for the particular source and entity in the entityrulemetadata
      val metaDataSchema = spark.table(properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME")).filter(lower(col("source")) === sourceName.toLowerCase() && lower(col("entity")) === entityName.toLowerCase()).filter(lower(col("rulename")) === "schemacheck")
      if (!metaDataSchema.take(1).isEmpty) {

        val body_col = df.columns.toSeq.head

        println("Applying Schemacheck\n______________")
        println("Time before Schemacheck--->" + Calendar.getInstance.getTime())
        return_Df = new Schemacheck().apply(df, schema, body_col, entityName, sourceName, spark, pipelineId, properties)
        println("Time after Schemacheck-->" + Calendar.getInstance.getTime())
      }
      else {
        println("Schemacheck not found for source=" + sourceName + " and entity=" + entityName + " in metadata. PLease make the required entry in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME") + " to run schecmacheck")
      }
    }
    else {
      val joinExceptionList=List("orphanedgecheck")
      val metaDataNonSchema = spark.table(properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME")).filter(lower(col("source")) === sourceName.toLowerCase() && lower(col("entity")) === entityName.toLowerCase()).filter(lower(col("rulename")) =!= "schemacheck")
      if (!metaDataNonSchema.take(1).isEmpty) {

        val originalDfColumns = df.columns.toSeq

        // getting the original columns of the dataframe (dq_uniqueID is added for internal framework purposes)
        if (containsIgnoreCase(originalDfColumns, "dq_uniqueID")) {
          println("Column Name: dq_uniqueID is reserved for internal use of the DQ Framework. Please pass a dataframe without this column.")
          return df
        }
        val dfUniqueId = df.withColumn("dq_uniqueID", monotonically_increasing_id())
        return_Df = dfUniqueId

        // splitting the task into threads to achieve parallel computation if multiple cores are available
        val forkJoinPool = new scala.concurrent.forkjoin.ForkJoinPool(Runtime.getRuntime().availableProcessors() * (spark.sparkContext.statusTracker.getExecutorInfos.length - 1))
        val lst = metaDataNonSchema.select("rulename", "parameters").collect().par
        lst.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

        lst.map({
          d =>
            //get relevant params like ruleName, params ,etc
            val ruleName = d.getString(0)
            val params = d.getString(1)
            val className = "com.ms.dq.rules." + ruleName.toLowerCase().capitalize
            var classObject: Class[_] = null

            //sanity check to see if the rule is provided correctly or not
            try {
              classObject = Class.forName(className)
            }
            catch {
              case e: java.lang.ClassNotFoundException => println("Cannot perform " + ruleName + ", as the required class doesn't exist. Please create required class in file " + className)
                println(e.printStackTrace())
            }

            if (classObject != null) {
              //printing out the line highlighting which rule is being performed currently and at what time
              //so we can also get an idea for how long did the DQ check run
              val instance = classObject.newInstance().asInstanceOf[ {def apply(df: Dataset[Row], params: String, entityname: String, sourcename: String, spark: SparkSession,pipelineId : String,properties: Properties): Dataset[Row]}]
              println("Applying " + ruleName + "\n___________")
              println("Time before " + entityName + " " + ruleName + "-->" + Calendar.getInstance.getTime())
              try {
                //applying the specific rule to the dataset
                val dfAfterRule = instance.apply(dfUniqueId, params, entityName, sourceName, spark, pipelineId, properties)

                //printing out the time after the DQ check in order to know how much time did the run take and returning the resulting dataframe:return_Df
                println("Time after " + entityName + " " + ruleName + "-->" + Calendar.getInstance.getTime())
                val addedCols = dfAfterRule.columns.toSeq.diff(originalDfColumns)
                if (!joinExceptionList.contains(ruleName)) {
                  //for thread-safe operation
                  synchronized {
                    return_Df = return_Df.join(dfAfterRule.select(addedCols.head, addedCols.tail: _*), "dq_uniqueID")
                  }
                }
              }
              catch {
                case e:java.lang.NoSuchMethodException => println("Cannot perform " + ruleName + ", as the required class " + className +" does not contain required method. Please create method def apply(df: Dataset[Row], params: String, entityname: String, sourcename: String, spark: SparkSession) in the class.")
                  println(e.printStackTrace())
              }
            }
        })
        return_Df = return_Df.drop("dq_uniqueID")
      }
      else {
        println("No Rulecheck found for source=" + sourceName + " and entity=" + entityName + " in metadata. PLease make the required entry in " + properties.getProperty("SUBJECT_AREA_DB") + "." + properties.getProperty("DQ_ENTITYRULEMETADATA_TABLE_NAME") + " to run the required rule checks.")
      }
    }
    return_Df
  }

  // Overloaded methods for optional parameters (schema & pipelineId) to invoke Data Quality rules on a dataset by reading the metadata table
  def applyDq(df: Dataset[Row], sourceName: String, entityName: String): Dataset[Row] = applyDqInner(df, sourceName, entityName, null, "N/A")

  // Overloaded methods for optional parameters (pipelineId) to invoke Data Quality rules on a dataset by reading the metadata table
  def applyDq(df: Dataset[Row], sourceName: String, entityName: String, schema: StructType): Dataset[Row] = applyDqInner(df, sourceName, entityName, schema, "N/A")

  def applyDq(df: Dataset[Row], sourceName: String, entityName: String, schema: StructType, pipelineId: String): Dataset[Row] = applyDqInner(df, sourceName, entityName, schema, pipelineId)

  // Overloaded methods for optional parameters (schema) to invoke Data Quality rules on a dataset by reading the metadata table
  def applyDq(df: Dataset[Row], sourceName: String, entityName: String, pipelineId: String): Dataset[Row] = applyDqInner(df, sourceName, entityName, null, pipelineId)

  // Method to invoke Data Quality rules on a dataset by passing the rules as a method parameter [ COMPUTE BOUND]
  def applyDq(df:Dataset[Row],rule_param_map: Map[String,String],colEntitySourceMap: Map[String, List[String]],pipelineId: String): Dataset[Row]={
    if (spark == null) {
      println("Please set spark session. Use function setSparkSession(_sparkSession:SparkSession)")
      throw new Exception("Please set spark session. Use function setSparkSession(_sparkSession:SparkSession)")
    }
    val originalDfColumns = df.columns.toSeq

    val properties = getProperties()

    //getting the original columns of the dataframe (dq_uniqueID is added for internal framework purposes)
    if (containsIgnoreCase(originalDfColumns, "dq_uniqueID")) {
      println("Column Name: dq_uniqueID is reserved for internal use of the DQ Framework. Please pass a dataframe without this column.")
      return df
    }
    val dfUniqueId = df.withColumn("dq_uniqueID", monotonically_increasing_id())
    var ans: Dataset[Row] = dfUniqueId

    // To achieve parallel computation if resources are there
    val forkJoinPool = new scala.concurrent.forkjoin.ForkJoinPool(Runtime.getRuntime().availableProcessors() * (spark.sparkContext.statusTracker.getExecutorInfos.length - 1))
    val lst = rule_param_map.par
    lst.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    lst.foreach { case (ruleName, params) =>
      //get relevant params like ruleName, params ,etc
      val className = "com.ms.dq.rules." + ruleName.toLowerCase().capitalize
      var classObject: Class[_] = null

      //sanity check to see if the rule is provided correctly or not
      try {
        classObject = Class.forName(className)
      }
      catch {
        case e: java.lang.ClassNotFoundException => println("Cannot perform " + ruleName + ", as the required class doesn't exist. Please create required class in file " + className)
          println(e.printStackTrace())
      }
      if (classObject != null) {
        //printing out the line highlighting which rule is being performed currently and at what time
        //so we can also get an idea for how long did the DQ check run
        val instance = classObject.newInstance().asInstanceOf[ {def apply(df: Dataset[Row], params: String, colEntitySourceMap: Map[String, List[String]], originalDfColumns: Seq[String], spark: SparkSession, pipelineId: String, properties: Properties): Dataset[Row]}]
        println("Applying " + ruleName + "\n___________")
        println("Time before " + ruleName + "-->" + Calendar.getInstance.getTime())
        try {
          //applying the specific rule to the dataset
          val dfAfterRule = instance.apply(dfUniqueId, params, colEntitySourceMap, originalDfColumns, spark, pipelineId, properties)

          //printing out the time after the DQ check in order to know how much time did the run take and returning the resulting dataframe:ans
          println("Time after " + ruleName + "-->" + Calendar.getInstance.getTime())
          val addedCols = dfAfterRule.columns.toSeq.diff(originalDfColumns)
          synchronized {
            ans = ans.join(dfAfterRule.select(addedCols.head, addedCols.tail: _*), "dq_uniqueID")
          }
        }
        catch {
          case e:java.lang.NoSuchMethodException => println("Cannot perform " + ruleName + ", as the required class " + className +" does not contain required method. Please create method def apply(df: Dataset[Row], params: String, colEntitySourceMap: Map[String, List[String]], originalDfColumns: Seq[String], spark: SparkSession): Dataset[Row] in the class.")
            println(e.printStackTrace())
        }
      }
    }
    ans.drop("dq_uniqueID")
  }
}