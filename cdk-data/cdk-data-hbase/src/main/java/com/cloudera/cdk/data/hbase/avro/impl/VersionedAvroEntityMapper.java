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
package com.cloudera.cdk.data.hbase.avro.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.KeySchema;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.dao.SchemaNotFoundException;
import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.hbase.EntitySerDe;
import com.cloudera.cdk.data.hbase.HBaseUtils;
import com.cloudera.cdk.data.hbase.KeySerDe;
import com.cloudera.cdk.data.hbase.PutAction;
import com.cloudera.cdk.data.hbase.manager.generated.ManagedSchemaEntityVersion;

/**
 * An entity mapper that is able to support multiple Avro schema versions in a
 * table. It uses a special set of columns in the table to determine the schema
 * version of each row, and uses that schema to properly deserialize the row.
 * Writing to tables is tied to a single schema.
 * 
 * @param <KEY>
 * @param <ENTITY>
 */
public class VersionedAvroEntityMapper<ENTITY extends IndexedRecord> implements
    EntityMapper<ENTITY> {

  private static Logger LOG = LoggerFactory
      .getLogger(VersionedAvroEntityMapper.class);

  /**
   * The schema parser we'll use to parse managed schemas.
   */
  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  /**
   * The Avro schema (represented as a string) which represents an entity
   * version.
   */
  private static final String managedSchemaEntityVersionSchema;
  static {
    managedSchemaEntityVersionSchema = AvroUtils
        .inputStreamToString(VersionedAvroEntityMapper.class
            .getResourceAsStream("/ManagedSchemaEntityVersion.avsc"));
  }

  private final SchemaManager schemaManager;
  private final String tableName;
  private final String entityName;
  private final Class<ENTITY> entityClass;
  private final AvroKeySchema keySchema;
  private final AvroEntitySchema entitySchema;
  private final int version;
  private final boolean specific;
  private final ConcurrentHashMap<Integer, EntityMapper<ENTITY>> entityMappers = new ConcurrentHashMap<Integer, EntityMapper<ENTITY>>();
  private EntityMapper<ManagedSchemaEntityVersion> managedSchemaEntityVersionEntityMapper;

  /**
   * Builder for the VersionedAvroEntityMapper. This is the only way to
   * construct one.
   */
  public static class Builder {

    private SchemaManager schemaManager;
    private String tableName;
    private String entityName;
    private boolean specific;
    private String genericSchemaString;

    public Builder setSchemaManager(SchemaManager schemaManager) {
      this.schemaManager = schemaManager;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setEntityName(String entityName) {
      this.entityName = entityName;
      return this;
    }

    public Builder setSpecific(boolean specific) {
      this.specific = specific;
      return this;
    }

    public Builder setGenericSchemaString(String genericSchemaString) {
      this.genericSchemaString = genericSchemaString;
      return this;
    }

    public <E extends IndexedRecord> VersionedAvroEntityMapper<E> build() {
      return new VersionedAvroEntityMapper<E>(this);
    }
  }

  @SuppressWarnings("unchecked")
  private VersionedAvroEntityMapper(Builder builder) {
    this.schemaManager = builder.schemaManager;
    this.tableName = builder.tableName;
    this.entityName = builder.entityName;
    this.specific = builder.specific;

    if (!specific) {
      // Must be a Generic avro record. Tie the key and entity classes to
      // GenericRecords
      entityClass = (Class<ENTITY>) GenericRecord.class;

      // Get the key schema to use from the schema manager.
      keySchema = (AvroKeySchema) schemaManager.getKeySchema(tableName,
          entityName);

      // The entiySchema to use for writes may have been passed into the
      // Builder. If not, use the highest version schema registered in the
      // schema manager.
      if (builder.genericSchemaString != null) {
        entitySchema = schemaParser.parseEntitySchema(builder.genericSchemaString);
      } else {
        entitySchema = (AvroEntitySchema) schemaManager.getEntitySchema(
            tableName, entityName);
      }

      // verify that this schema exists in the managed schema table, and get its
      // version. This method will throw a SchemaNotFoundException if it can't
      // find it.
      version = schemaManager.getEntityVersion(tableName, entityName,
          entitySchema);
    } else {
      // specific record, so initialize the schema, class, and version member
      // variables appropriately.
      try {
        keySchema = (AvroKeySchema) schemaManager.getKeySchema(tableName,
            entityName);
        // Get the highest version schema to determine the key and entity record
        // class names. Initialize the key and entity class members from those
        // names.
        AvroEntitySchema mostRecentEntitySchema = (AvroEntitySchema) schemaManager
            .getEntitySchema(tableName, entityName);
        String entityClassName = mostRecentEntitySchema.getAvroSchema()
            .getFullName();
        entityClass = (Class<ENTITY>) Class.forName(entityClassName);

        // Initialize the entitySchema from the SCHEMA$ field on the class. This
        // will be or schema we'll use to write with.
        String entitySchemaString = entityClass.getField("SCHEMA$").get(null)
            .toString();
        entitySchema = schemaParser.parseEntitySchema(entitySchemaString);

        // verify that this schema exists in the managed schema table, and get
        // its
        // version. This method will throw a SchemaNotFoundException if it can't
        // find it.
        version = schemaManager.getEntityVersion(tableName, entityName,
            entitySchema);
      } catch (ClassNotFoundException e) {
        String msg = "Key or entity class not found. Make sure the specific "
            + "record instances are on the classpath.";
        LOG.error(msg, e);
        throw new HBaseCommonException(msg, e);
      } catch (SecurityException e) {
        String msg = "Cannot access key or entity class.";
        LOG.error(msg, e);
        throw new HBaseCommonException(msg, e);
      } catch (NoSuchFieldException e) {
        String msg = "SCHEMA$ field not found in the entity class";
        LOG.error(msg, e);
        throw new HBaseCommonException(msg, e);
      } catch (IllegalAccessException e) {
        String msg = "Not allowed to access SCHEMA$ field in the entity class";
        LOG.error(msg, e);
        throw new HBaseCommonException(msg, e);
      }
    }

    // Initialize the entity mappers this object wraps. There will be one entity
    // mapper per version of the schema. When deserializing a row, we'll use the
    // entity mapper that configured for that schema version.
    updateEntityMappers();

    // Initialize the entity mapper used to deserialize the special
    // ManagedSchemaEntityVersion record from each row.
    initializeEntityVersionEntityMapper();
  }

  @Override
  public ENTITY mapToEntity(Result result) {
    ManagedSchemaEntityVersion versionRecord = managedSchemaEntityVersionEntityMapper
        .mapToEntity(result);
    int resultVersion = 0;
    if (versionRecord != null
        && versionRecord.getSchemaVersion().containsKey(entityName)) {
      resultVersion = versionRecord.getSchemaVersion().get(entityName);
    }
    if (entityMappers.containsKey(resultVersion)) {
      return entityMappers.get(resultVersion).mapToEntity(result);
    } else {
      schemaManager.refreshManagedSchemaCache(tableName, entityName);
      updateEntityMappers();
      if (entityMappers.containsKey(resultVersion)) {
        return entityMappers.get(resultVersion).mapToEntity(result);
      } else {
        String msg = "Could not find schema for " + tableName + ", "
            + entityName + ", with version " + resultVersion;
        LOG.error(msg);
        throw new SchemaNotFoundException(msg);
      }
    }
  }

  @Override
  public PutAction mapFromEntity(ENTITY entity) {
    EntityMapper<ENTITY> entityMapper = entityMappers.get(version);
    PutAction entityPut = entityMapper.mapFromEntity(entity);

    ManagedSchemaEntityVersion versionRecord = ManagedSchemaEntityVersion
        .newBuilder().setSchemaVersion(new HashMap<String, Integer>()).build();
    versionRecord.getSchemaVersion().put(entityName, version);
    PutAction versionPut = managedSchemaEntityVersionEntityMapper
        .mapFromEntity(versionRecord);
    
    byte[] keyBytes = entityPut.getPut().getRow();
    versionPut = HBaseUtils.mergePutActions(keyBytes, Arrays.asList(versionPut));
    return HBaseUtils.mergePutActions(keyBytes,
        Arrays.asList(entityPut, versionPut));
  }

  @Override
  public Increment mapToIncrement(PartitionKey key, String fieldName, long incrementValue) {
    return entityMappers.get(version).mapToIncrement(key, fieldName,
        incrementValue);
  }

  @Override
  public long mapFromIncrementResult(Result result, String fieldName) {
    return entityMappers.get(version).mapFromIncrementResult(result, fieldName);
  }

  @Override
  public Set<String> getRequiredColumns() {
    Set<String> requiredColumns = entityMappers.get(version)
        .getRequiredColumns();
    requiredColumns.addAll(managedSchemaEntityVersionEntityMapper
        .getRequiredColumns());
    return requiredColumns;
  }

  @Override
  public Set<String> getRequiredColumnFamilies() {
    Set<String> requiredColumns = entityMappers.get(version)
        .getRequiredColumnFamilies();
    requiredColumns.addAll(managedSchemaEntityVersionEntityMapper
        .getRequiredColumnFamilies());
    return requiredColumns;
  }

  @Override
  public KeySchema getKeySchema() {
    return keySchema;
  }

  @Override
  public EntitySchema getEntitySchema() {
    return entitySchema;
  }

  @Override
  public KeySerDe getKeySerDe() {
    return entityMappers.get(version).getKeySerDe();
  }

  @Override
  public EntitySerDe<ENTITY> getEntitySerDe() {
    return entityMappers.get(version).getEntitySerDe();
  }

  /**
   * Initialize the entity mapper we'll use to convert the schema version
   * metadata in each row to a ManagedSchemaEntityVersion record.
   */
  private void initializeEntityVersionEntityMapper() {
    AvroKeySerDe keySerDe = null;
    for (EntityMapper<ENTITY> entityMapper : entityMappers.values()) {
      keySerDe = (AvroKeySerDe) entityMapper.getKeySerDe();
      break;
    }
    AvroEntitySchema avroEntitySchema = schemaParser
        .parseEntitySchema(managedSchemaEntityVersionSchema);
    avroEntitySchema = AvroUtils.mergeSpecificStringTypes(
        ManagedSchemaEntityVersion.class, avroEntitySchema);
    AvroEntityComposer<ManagedSchemaEntityVersion> entityComposer = new AvroEntityComposer<ManagedSchemaEntityVersion>(
        avroEntitySchema, true);
    AvroEntitySerDe<ManagedSchemaEntityVersion> entitySerDe = new AvroEntitySerDe<ManagedSchemaEntityVersion>(
        entityComposer, avroEntitySchema, avroEntitySchema, true);
    this.managedSchemaEntityVersionEntityMapper = new BaseEntityMapper<ManagedSchemaEntityVersion>(
        keySchema, avroEntitySchema, keySerDe, entitySerDe);
  }

  /**
   * Update the map of wrapped entity mappers to reflect the most recent entity
   * schema metadata returned by the schemaManager.
   */
  private void updateEntityMappers() {
    for (Entry<Integer, EntitySchema> entry : schemaManager.getEntitySchemas(
        tableName, entityName).entrySet()) {
      if (!entityMappers.containsKey(entry.getKey())) {
        AvroEntitySchema writtenSchema = (AvroEntitySchema) entry.getValue();
        EntityMapper<ENTITY> entityMapper = constructWrappedEntityMapper(
            keySchema, entitySchema, writtenSchema, entityClass);
        entityMappers.put(entry.getKey(), entityMapper);
      }
    }
  }

  /**
   * 
   * @param keySchema
   * @param readSchema
   * @param writeSchema
   * @param keyClass
   * @param entityClass
   * @return
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private EntityMapper<ENTITY> constructWrappedEntityMapper(
      AvroKeySchema keySchema, AvroEntitySchema readSchema,
      AvroEntitySchema writeSchema, Class entityClass) {
    if (specific) {
      keySchema = AvroUtils.mergeSpecificStringTypes(entityClass, keySchema);
      readSchema = AvroUtils.mergeSpecificStringTypes(entityClass, readSchema);
      AvroEntityComposer entityComposer = new AvroEntityComposer(readSchema,
          true);
      AvroEntitySerDe entitySerDe = new AvroEntitySerDe(entityComposer,
          readSchema, writeSchema, true);
      KeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
      return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
    } else {
      KeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
      AvroEntityComposer entityComposer = new AvroEntityComposer(readSchema,
          false);
      AvroEntitySerDe entitySerDe = new AvroEntitySerDe(entityComposer,
          readSchema, writeSchema, false);
      return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
    }
  }
}
