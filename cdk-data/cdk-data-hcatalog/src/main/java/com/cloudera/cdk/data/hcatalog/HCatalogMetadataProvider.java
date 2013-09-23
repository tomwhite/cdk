/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.data.hcatalog;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetExistsException;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.MetadataProviderException;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractMetadataProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .PrimitiveObjectInspectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HCatalogMetadataProvider extends AbstractMetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogMetadataProvider.class);

  private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  private static final String PARTITION_EXPRESSION_PROPERTY_NAME = "cdk.partition.expression";
  private static final String AVRO_SCHEMA_URL_PROPERTY_NAME = "avro.schema.url";
  private static final String AVRO_SCHEMA_LITERAL_PROPERTY_NAME = "avro.schema.literal";

  private final boolean managed;
  private final Configuration conf;
  private final HCatalog hcat;
  private final String dbName = "default";
  private FileSystem fileSystem;
  private Path dataDirectory;

  public HCatalogMetadataProvider(boolean managed) {
    this.managed = managed;
    this.conf = new Configuration();
    hcat = new HCatalog();
  }

  public HCatalogMetadataProvider(boolean managed, Configuration conf) {
    this.managed = managed;
    this.conf = conf;
    hcat = new HCatalog(conf);
  }

  @Override
  public DatasetDescriptor load(String name) {
    final Table table = hcat.getTable(dbName, name);

    String serializationLib = table.getSerializationLib();
    if (!AVRO_SERDE.equals(serializationLib)) {
      throw new MetadataProviderException("Only tables using AvroSerDe are supported.");
    }

    try {
      fileSystem = FileSystem.get(conf);
      dataDirectory = fileSystem.makeQualified(new Path(table.getDataLocation()));
    } catch (IOException e) {
      throw new MetadataProviderException(e);
    }

    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();
    if (table.getProperty(PARTITION_EXPRESSION_PROPERTY_NAME) != null) {
      builder.partitionStrategy(Accessor.getDefault().fromExpression(table.getProperty
          (PARTITION_EXPRESSION_PROPERTY_NAME)));
    }
    String schemaUrlString = table.getProperty(AVRO_SCHEMA_URL_PROPERTY_NAME);
    if (schemaUrlString != null) {
      try {
        URI schemaUrl = new URI(schemaUrlString);
        return builder.schema(schemaUrl).get();
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      } catch (URISyntaxException e) {
        throw new MetadataProviderException(e);
      }
    }
    String schemaLiteral = table.getProperty(AVRO_SCHEMA_LITERAL_PROPERTY_NAME);
    if (schemaLiteral != null) {
      return builder.schema(schemaLiteral).get();
    }
    throw new MetadataProviderException("Can't find schema.");

  }

  FileSystem getFileSystem() {
    return fileSystem;
  }

  Path getDataDirectory() {
    return dataDirectory;
  }

  void setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  void setDataDirectory(Path dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    if (!managed) {
      // this table is external and the data directory must be set
      Preconditions.checkArgument(
          dataDirectory != null,
          "Cannot create an external table: dataDirectory is null");
    }

    if (hcat.tableExists(dbName, name)) {
      throw new DatasetExistsException(
          "Metadata already exists for dataset:" + name);
    }

    logger.info("Creating a Hive table named: " + name);
    Table tbl = new Table(dbName, name);
    tbl.setTableType(managed ? TableType.MANAGED_TABLE : TableType.EXTERNAL_TABLE);
    try {
      if (dataDirectory != null) {
        tbl.setDataLocation(fileSystem.makeQualified(dataDirectory).toUri());
      }
      tbl.setSerializationLib(AVRO_SERDE);
      tbl.setInputFormatClass("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
      tbl.setOutputFormatClass("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
      if (descriptor.getSchemaUrl() != null) {
        tbl.setProperty(AVRO_SCHEMA_URL_PROPERTY_NAME, descriptor.getSchemaUrl().toExternalForm());
      } else {
        tbl.setProperty(AVRO_SCHEMA_LITERAL_PROPERTY_NAME, descriptor.getSchema().toString());
      }
      if (descriptor.isPartitioned()) {
        PartitionStrategy ps = descriptor.getPartitionStrategy();
        tbl.setProperty(PARTITION_EXPRESSION_PROPERTY_NAME,
            Accessor.getDefault().toExpression(ps));
        List<FieldSchema> partCols = Lists.newArrayList();
        for (FieldPartitioner fp : ps.getFieldPartitioners()) {
          partCols.add(new FieldSchema(fp.getName(), getHiveType(fp.getType()),
              "Partition column derived from '" + fp.getSourceName() + "' column, " +
                  "generated by CDK."));
        }
        tbl.setPartCols(partCols);
      }
    } catch (Exception e) {
      throw new MetadataProviderException("Error configuring Hive Avro table, " +
          "table creation failed", e);
    }
    hcat.createTable(tbl);

    if (dataDirectory == null) { // re-read to find the data directory
      Table table = hcat.getTable(dbName, name);
      try {
        fileSystem = FileSystem.get(conf);
        dataDirectory = fileSystem.makeQualified(new Path(table.getDataLocation()));
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }

    return descriptor;
  }

  private String getHiveType(Class<?> type) {
    String typeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(type);
    if (typeName == null) {
      throw new MetadataProviderException("Unsupported FieldPartitioner type: " + type);
    }
    return typeName;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean delete(String name) {
    if (!hcat.tableExists(dbName, name)) {
      return false;
    }
    hcat.dropTable(dbName, name);
    return true;
  }

  @Override
  public boolean exists(String name) {
    return hcat.exists(dbName, name);
  }

  protected Collection<String> list() {
    return hcat.getAllTables(dbName);
  }
}
