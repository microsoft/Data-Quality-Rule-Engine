# Overloaded Methods for Metadata Driven Data Quality Check
### applydq(df: Dataset[Row],sourcename:String,entityname:String): Dataset[Row] :  
* Overloaded method that would read the metadata to fetch the rules to be applied, excluding schema check.
* Optional parameters (schema and pipelineId) would be defaulted. 
### applydq(df: Dataset[Row],sourcename:String,entityname:String, schema: StructType): Dataset[Row] :  
* Overloaded method that would read the metadata to fetch the rules to be applied, including schema check.
* Optional parameters (pipelineId) would be defaulted.
### applydq(df: Dataset[Row],sourcename:String,entityname:String,schema: StructType , pipelineId: String): Dataset[Row] :  
* Overloaded method that would read the metadata to fetch the rules to be applied, including schema check. 
### applydq(df: Dataset[Row],sourcename:String,entityname:String, pipelineId: String): Dataset[Row] :  
* Overloaded method that would read the metadata to fetch the rules to be applied, excluding schema check. 
* Optional parameters (schema) would be defaulted.

