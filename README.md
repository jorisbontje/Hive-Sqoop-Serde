# Hive SerDe for accessing Sqoop sequence files.

Extracted from https://issues.cloudera.org/browse/SQOOP-171

Hive SerDe for accessing Sqoop sequence files. To use generate a sequence
file using sqoop and then in Hive create the table. An example DDL statement
would be:

```
CREATE EXTERNAL TABLE table_name (
  id INT,
  name STRING
)
ROW FORMAT SERDE 'com.cloudera.sqoop.contrib.FieldMappableSerDe'
WITH SERDEPROPERTIES (
  "fieldmappable.classname" = "name.of.FieldMappable.generated.by.sqoop"
)
STORED AS SEQUENCEFILE
LOCATION "hdfs://hdfs.server/path/to/sequencefile";
```
