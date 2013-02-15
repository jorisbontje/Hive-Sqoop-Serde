/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sqoop.contrib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import org.apache.sqoop.lib.FieldMappable;
/**
 * Hive SerDe for accessing Sqoop sequence files. To use generate a sequence
 * file using sqoop and then in Hive create the table. An example DDL statement
 * would be:
 * <pre>
 * CREATE EXTERNAL TABLE table_name (
 *   id INT,
 *   name STRING
 * )
 * ROW FORMAT SERDE 'com.cloudera.sqoop.contrib.FieldMappableSerDe'
 * WITH SERDEPROPERTIES (
 *   "fieldmappable.classname" = "name.of.FieldMappable.generated.by.sqoop"
 * )
 * STORED AS SEQUENCEFILE
 * LOCATION "hdfs://hdfs.server/path/to/sequencefile";
 * </pre>
 */
public class FieldMappableSerDe implements SerDe {
  public static final Log LOG = LogFactory.getLog(FieldMappableSerDe.class.getName());

  private int numColumns;

  private StructObjectInspector rowOI;
  private ArrayList<Object> row;

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private Class<? extends Writable> fieldMappableClass;

  @Override
  public void initialize(Configuration conf, Properties props) throws SerDeException {
    String columnNameProperty = props.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String fieldMappableClassname = props.getProperty("fieldmappable.classname");

    try {
      fieldMappableClass = findWritableClassObject(fieldMappableClassname);
    } catch (ClassNotFoundException e) {
      throw new SerDeException(e);
    }

    columnNames = Arrays.asList(columnNameProperty.split(","));
    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c)));
    }

    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<Object>(numColumns);
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends Writable> findWritableClassObject(String fieldMappableClassname)
    throws ClassNotFoundException {
    return (Class<? extends Writable>) Class.forName(fieldMappableClassname);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    FieldMappable fieldMappable = (FieldMappable) writable;
    Map<String, Object> fieldMap = fieldMappable.getFieldMap();
    int columnIndex = 0;
    for (String columnName : columnNames) {
      if (fieldMap.containsKey(columnName)) {
        Object value = fieldMap.get(columnName);
        row.set(columnIndex, value);
      } else {
        row.set(columnIndex, null);
        LOG.warn("Row does not contain column named '" + columnName + "'");
      }
      columnIndex++;
    }
    return row;
  }

  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Serializer.serialize");
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return fieldMappableClass;
  }

  public SerDeStats getSerDeStats() {
    return new SerDeStats();
  }

}
