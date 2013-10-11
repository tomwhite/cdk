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
package com.cloudera.cdk.data.hbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.dao.KeySchema;

/**
 * Base implementation of the CompositeDao interface. Internally managed
 * multiple EntityMappers that each handle their respective sub types.
 * 
 * @param <K>
 *          The type of the key
 * @param <E>
 *          The type of the entity this DAO returns. This entity will be a
 *          composition of the sub entities.
 * @param <S>
 *          The type of the sub entities.
 */
public abstract class CompositeBaseDao<E, S> implements
    CompositeDao<E, S> {

  private final BaseDao<E> baseDao;

  /**
   * An EntityMapper implementation that will map to and from multiple entities
   * per row. These are the sub entities that make up the composed entity.
   */
  private class CompositeEntityMapper implements EntityMapper<E> {

    private final List<EntityMapper<S>> entityMappers;

    public CompositeEntityMapper(List<EntityMapper<S>> entityMappers) {
      if (entityMappers.size() == 0) {
        throw new IllegalArgumentException(
            "Must provide more than one entity mapper to CompositeEntityMapper");
      }
      this.entityMappers = entityMappers;
    }

    @Override
    public E mapToEntity(Result result) {
      List<S> entityList = new ArrayList<S>();
      for (EntityMapper<S> entityMapper : entityMappers) {
        S entity = entityMapper.mapToEntity(result);
        // could be null. compose will handle a null sub entity appropriately.
        entityList.add(entity);
      }
      return compose(entityList);
    }

    @Override
    public PutAction mapFromEntity(E entity) {
      List<PutAction> puts = new ArrayList<PutAction>();
      List<S> subEntities = decompose(entity);
      byte[] keyBytes = null;
      for (int i = 0; i < entityMappers.size(); i++) {
        S subEntity = subEntities.get(i);
        if (subEntity != null) {
          PutAction put = entityMappers.get(i).mapFromEntity(subEntity);
          if (keyBytes == null) {
            keyBytes = put.getPut().getRow();
          }
          puts.add(entityMappers.get(i).mapFromEntity(subEntity));
        }
      }
      return HBaseUtils.mergePutActions(keyBytes, puts);
    }

    @Override
    public Increment mapToIncrement(PartitionKey key, String fieldName, long amount) {
      throw new UnsupportedOperationException(
          "We don't currently support increment on CompositeDaos");
    }

    @Override
    public long mapFromIncrementResult(Result result, String fieldName) {
      throw new UnsupportedOperationException(
          "We don't currently support increment on CompositeDaos");
    }

    @Override
    public Set<String> getRequiredColumns() {
      Set<String> requiredColumnsSet = new HashSet<String>();
      for (EntityMapper<?> entityMapper : entityMappers) {
        requiredColumnsSet.addAll(entityMapper.getRequiredColumns());
      }
      return requiredColumnsSet;
    }

    @Override
    public Set<String> getRequiredColumnFamilies() {
      Set<String> requiredColumnFamiliesSet = new HashSet<String>();
      for (EntityMapper<?> entityMapper : entityMappers) {
        requiredColumnFamiliesSet.addAll(entityMapper
            .getRequiredColumnFamilies());
      }
      return requiredColumnFamiliesSet;
    }

    @Override
    public KeySchema getKeySchema() {
      return entityMappers.get(0).getKeySchema();
    }

    @Override
    public EntitySchema getEntitySchema() {
      boolean transactional = entityMappers.get(0).getEntitySchema()
          .isTransactional();
      List<String> tables = new ArrayList<String>();
      List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
      for (EntityMapper<?> entityMapper : entityMappers) {
        tables.addAll(entityMapper.getEntitySchema().getTables());
        fieldMappings.addAll(entityMapper.getEntitySchema().getFieldMappings());
      }
      return new EntitySchema(tables, null, fieldMappings,
          transactional);
    }

    @Override
    public KeySerDe getKeySerDe() {
      return entityMappers.get(0).getKeySerDe();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public EntitySerDe getEntitySerDe() {
      return entityMappers.get(0).getEntitySerDe();
    }
  }

  /**
   * Constructor that will internally create an HBaseClientTemplate from the
   * tablePool and the tableName.
   * 
   * @param tablePool
   *          A pool of HBase Tables.
   * @param tableName
   *          The name of the table this dao persists to and fetches from.
   * @param entityMappers
   *          Maps between entities and the HBase operations for their
   *          respective sub entities.
   */
  public CompositeBaseDao(HTablePool tablePool, String tableName,
      List<EntityMapper<S>> entityMappers) {
    baseDao = new BaseDao<E>(tablePool, tableName,
        new CompositeEntityMapper(entityMappers));
  }

  @Override
  public E get(PartitionKey key) {
    return baseDao.get(key);
  }

  @Override
  public boolean put(E entity) {
    return baseDao.put(entity);
  }

  @Override
  public long increment(PartitionKey key, String fieldName, long amount) {
    throw new UnsupportedOperationException(
        "We don't currently support increment on CompositeDaos");
  }

  @Override
  public void delete(PartitionKey key) {
    baseDao.delete(key);
  }

  @Override
  public boolean delete(PartitionKey key, E entity) {
    return baseDao.delete(key, entity);
  }

  @Override
  public EntityScanner<E> getScanner() {
    return baseDao.getScanner();
  }

  @Override
  public EntityScanner<E> getScanner(PartitionKey startKey,
      PartitionKey stopKey) {
    return baseDao.getScanner(startKey, stopKey);
  }

  @Override
  public KeySchema getKeySchema() {
    return baseDao.getKeySchema();
  }

  @Override
  public EntitySchema getEntitySchema() {
    return baseDao.getEntitySchema();
  }

  @Override
  public EntityBatch<E> newBatch(long writeBufferSize) {
    return baseDao.newBatch(writeBufferSize);
  }

  @Override
  public EntityBatch<E> newBatch() {
    return baseDao.newBatch();
  }
  
  @Override
  public PartitionStrategy getPartitionStrategy() {
    return baseDao.getPartitionStrategy();
  }

}
