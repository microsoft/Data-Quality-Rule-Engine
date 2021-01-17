// Databricks notebook source
// DBTITLE 1,Create Data Frame
val df=spark.sql(s"""select 1 as id,2 as partitionkey,'John' as Name,CAST('2020-12-01T21:06:20.000+0000' as Timestamp) as EventDateTime
union all
select 2 as id, 3 as partitionkey, 'Jack' as Name,current_date as EventDateTime
union all 
select 3 as id, 4 as partitionkey, null as Name,current_date as EventDateTime
union all 
select 1 as id,2 as partitionkey,'John' as Name,current_date as EventDateTime""")
display(df)

// COMMAND ----------

// DBTITLE 1,Add Entries for Metadata Drive Data Quality Check
// MAGIC %sql
// MAGIC insert into dq.entityrulemetadata 
// MAGIC (select 'sourceName' as source,'entityName' as entity,'nullcheck' as rulename, "{\"columnList\":\"name,eventDateTime\"}" as parameters
// MAGIC union all
// MAGIC select 'sourceName' as source,'entityName' as entity,'uniquecheck' as rulename, "{\"columnList\":[\"id\",\"partitionkey\"],\"LatestRowIdentifier\":\"eventDateTime\"}" as parameters)

// COMMAND ----------

// DBTITLE 1,Metadata Driven Data Quality Check
//metadata driven
import com.ms.jedi.dq.framework.DQFramework    
val dqObj=new DQFramework()
dqObj.setSparkSession(spark)
val afterDq=dqObj.applyDq(df,"sourceName","entityName")
display(afterDq)

// COMMAND ----------

// DBTITLE 1,Parameter Driven Data Quality  Check
//parameter driven
import com.ms.jedi.dq.framework.DQFramework    
val dqObj=new DQFramework()
dqObj.setSparkSession(spark)
val col_rule_map=Map("nullcheck"->"""{"columnList":"name,eventDateTime"}""",
                    "uniquecheck"->"""{"columnList":["id,partitionkey"],"LatestRowIdentifier":"eventDateTime"}""")
val map:Map[String,List[String]]=Map("id,partitionkey"->List("entity1","source1"),
                                    "eventDateTime"->List("entity2","source2"),
                                    "name"->List("entity3","source3"))
val afterDq=dqObj.applyDq(df,col_rule_map,map,"pipelineid")
display(afterDq.select("id","partitionkey","name","eventDateTime","name_nullcheck","eventDateTime_nullcheck","id_partitionkey_uniquecheck","LatestRow_id_partitionkey"))

// COMMAND ----------

// DBTITLE 1,View Reporting data
// MAGIC %sql
// MAGIC select * from dq.dqrundetails where source in ('source1','source2','source3','sourceName')

// COMMAND ----------

// DBTITLE 1,View Reporting data
// MAGIC %sql
// MAGIC select * from dq.dqfailtable where source in ('source1','source2','source3','sourceName')

// COMMAND ----------

// DBTITLE 1,View Reporting data
// MAGIC %sql
// MAGIC select * from dq.dqAggTable where source in ('source1','source2','source3','sourceName')

// COMMAND ----------


