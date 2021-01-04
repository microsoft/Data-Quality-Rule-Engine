# Data Quality Rule Engine
The data quality rule engine is a library that will help run data quality checks on datasets.

### Data Quality Rules implemented:
*	Null Check: Checks whether the values of the column are null or not. 
*	Unique Check: Checks whether the values of the columns are unique or not. Optionally, it may also check for the latest row for a particular value of a column.
*	Orphan Edge Check: Checks whether the dataframe’s values referring to an id of a parent dataframe, exist in the parent dataframe.
*	Schema Check: Checks whether the json representing the values of the dataframe follow a given schema.

There are 2 ways of invoking the Data Quality Rules on a dataframe:
*	Metadata driven.
*	As a parameter to the function call.
The details of the Data Quality runs, and the erroneous records are maintained in reporting tables. However, if the User wishes to not have these recorded, the DQ_LOG_RESULTS_FLAG in the application.properties file can be set to false.

### Requirements:
The Data Quality Rule Engine is compatible with 
*	Apache Spark version 3.0.x
*	Java 8
*	Databricks Cluster with Spark 3.0.x configuration (used only for schema check).

### To use the Data Quality Rule Engine in a project:
1. Install the Data Quality Rule Engine to a repository.

2. Add a reference to the repository in pom.xml of your project under the tag:
	`<repository>`

3. Add the framework as a dependency to the pom.xml of your project (Make sure you have dependency to org.scala-lang.):
```
<dependency> 
   <groupId>com.ms.dq</groupId> 
   <artifactId>DQFramework</artifactId> 
   <version>LATEST</version> 
</dependency>
```

4. Then, simply import the class in your code:
  
`import com.ms.dq.framework.DQFramework`

### To use the Data Quality Rule Engine as a library in the Databricks notebook:

Build the Data Quality Rule Engine jar and upload the library to the Databricks cluster.
Then, simply import the class in the notebook:

`import com.ms.dq.framework.DQFramework`
  

### Metadata Driven:
The Data Quality rules to be applied on a data frame are inserted into metadata tables (details below). The Rule Engine relies on the metadata to execute the rules.
Note that the table names can be configured as per the user’s requirement by updating the application.properties file.
*	Entityrulemetadata: A delta table of schema type (source, entity, rule, parameters). For a rule to be executed against a particular entity, an entry for that rule must be present in this table. The parameters field basically contains a JSON of key value pairs pertinent to the rule. The keys for the parameters of the 4 rules are mentioned below and a snippet of the data in the table is attached. 
	  * Null Check
        * columnList: String. A comma separated list of the columns to apply null check on.
    * Unique Check
        * columnList: String. A comma separated list of the columns to apply unique check on.
        * LatestRowIdentifier (Optional): String. An optional key in case you want the framework to return whether the record is the latest unique row for that column or not as well. Basically, refers to the column which should be used as an identifier for the latest row.
     * Orphan Edge Check
        * tableName: String. The delta table name for the source, entity on which DQ will run. 
        * auditColIntName: String. The audit column which will be used for delta identification. (Must be the audit column in int). 
        * cutOffHours(optional): Int. Default 0. Delta should be considered from these many hours before the run.
        * cutOffMinutes(optional): Int. Default 0. Delta should be considered from these many minutes before the run.
        * cutOffSeconds(optional): Int. Default 0. Delta should be considered from these many seconds before the run.
 
       ![EntityRuleMwtadata](https://github.com/microsoft/Data-Quality-Rule-Engine/blob/main/images/Entityrulemetadata.png)

* Orphanedgemetadata: A metadata table for the orphan edge check. For the orphan edge check to run on a particular source, entity a record for that source, entity must be present in this metadata table. Sample records are provided below:

     ![OrphanEdgeMetadata](https://github.com/microsoft/Data-Quality-Rule-Engine/blob/main/images/OrphanEdgeMetadata.png)

#### To invoke the Rule Engine:

1.	Create an object of the framework:

 	   `val dqObj=new DQFramework()`

2.	Set the Spark session:

	   `dqObj.setSparkSession(spark)`

3.	Make the required entries in the metadata tables, as explained in section [Metadata Driven](https://github.com/microsoft/Data-Quality-Rule-Engine#metadata-driven).
4.	Invoke the method to apply the rules:

     `val applyDq = dqObj.applyDq(dataframe,source,entity,schema,pipelineId)`

Please refer the [DQFramework readme file](https://github.com/microsoft/Data-Quality-Rule-Engine/blob/main/dq/src/main/scala/com/ms/dq/framework/README.md) for the available overloaded methods for applyDq().


### Parameter Based:

Another option to invoke the Data Quality Rules is by invoking the applyDQ() with the required parameters.

1.	Define a mapping of the rules and columns of the data frame.
    ```
    val col_rule_map=Map(
      "nullcheck" -> """{"columnList":" id,createdDateTime"}""",
      "uniquecheck" -> """{"columnList":" ["id,partitionKey", "id"]",”latestrowidentifier“:”ModifiedDate”}""")
    ```

2.	Provide a mapping of the columns on which the rules are applied and the respective source and entity.

      ```
      val map:Map[String,List[String]]=Map("id"->List("source1","entity1"),
        "createdDateTime "->List("source1"," entity1"),
        "id"->List("source1"," entity1"),
        "partitionKey"->List("source1"," entity1"))
      ```

3.	Invoke the method to apply the rules:

     `val applyDq = dqObj.applyDq(dataframe,col_rule_map,map,pipelineId)`



### Results:
The dataframe returned consists of the original dataframe with additional columns for each (rule, column) pair with the name rulename_columnname. These columns are Boolean fields representing whether the row is valid or not. 

For example, nullcheck is applied on createdDate, a new column named createdDate_nullcheck will be present in the dataframe returned. In the rows for which this column has the value true, createdDate values are not null, and for the ones in which they are false, the createdDate value is null. 

   ![Results](https://github.com/microsoft/Data-Quality-Rule-Engine/blob/main/images/Results.png) 



## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

### Bugs and Feature requests:
Please use the [GitHub issue tracker](https://github.com/microsoft/Data-Quality-Rule-Engine/issues) to file bugs or feature requests with relevant information.

### Creating a Pull Request:
1.	[Create a fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) of the repository.
2.	Make the required changes, following the existing code conventions.
3.	Ensure the unit test cases pass.
4.	Commit the changes to your fork.
5.	[Create a pull request](https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request), with details of the unit tests.
6.	When you submit a pull request, a CLA-bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repositories using our CLA.


This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
