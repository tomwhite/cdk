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

import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.KeySchema;
import com.cloudera.cdk.data.dao.PartialKey;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * A DAO implementation that uses a constructor provided EntityMapper to do
 * basic conversion between entities and HBase Gets and Puts.
 * 
 * @param <K>
 *          The key type
 * @param <E>
 *          The entity type.
 */
public class BaseDao<K, E> implements Dao<K, E> {

  private final String tableName;
  private final EntityMapper<K, E> entityMapper;
  private final HBaseClientTemplate clientTemplate;

  /**
   * Constructor that will internally create an HBaseClientTemplate from the
   * tablePool and the tableName.
   * 
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          A pool of HBase Tables.
   * @param tableName
   *          The name of the table this dao persists to and fetches from.
   * @param entityMapper
   *          Maps between entities and the HBase operations.
   */
  public BaseDao(HTablePool tablePool, String tableName,
      EntityMapper<K, E> entityMapper) {
    this.tableName = tableName;
    this.entityMapper = entityMapper;
    this.clientTemplate = new HBaseClientTemplate(tablePool, tableName);
  }

  /**
   * Constructor that copies an existing dao.
   * 
   * @param dao
   *          Dao to copy.
   */
  public BaseDao(BaseDao<K, E> dao) {
    this.tableName = dao.tableName;
    this.clientTemplate = new HBaseClientTemplate(dao.clientTemplate);
    this.entityMapper = dao.entityMapper;
  }

  @Override
  public E get(K key) {
    return clientTemplate.get(key, entityMapper);
  }

  @Override
  public boolean put(K key, E entity) {
    return clientTemplate.put(key, entity, entityMapper);
  }

  @Override
  public long increment(K key, String fieldName, long amount) {
    return clientTemplate.increment(key, fieldName, amount, entityMapper);
  }

  public void delete(K key) {
    clientTemplate.delete(key, entityMapper.getRequiredColumns(), null,
        entityMapper.getKeySerDe());
  }

  @Override
  public boolean delete(K key, E entity) {
    VersionCheckAction checkAction = entityMapper.mapFromEntity(key, entity)
        .getVersionCheckAction();
    return clientTemplate.delete(key, entityMapper.getRequiredColumns(),
        checkAction, entityMapper.getKeySerDe());
  }

  @Override
  public EntityScanner<K, E> getScanner() {
    return getScanner((K) null, null);
  }

  @Override
  public EntityScanner<K, E> getScanner(K startKey, K stopKey) {
    return clientTemplate.getScannerBuilder(entityMapper).setStartKey(startKey)
        .setStopKey(stopKey).build();
  }

  @Override
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey,
      PartialKey<K> stopKey) {
    return clientTemplate.getScannerBuilder(entityMapper)
        .setPartialStartKey(startKey).setPartialStopKey(stopKey).build();
  }

  public EntityScannerBuilder<K, E> getScannerBuilder() {
    return clientTemplate.getScannerBuilder(entityMapper);
  }

  @Override
  public EntityBatch<K, E> newBatch(long writeBufferSize) {
    return clientTemplate.createBatch(entityMapper, writeBufferSize);
  }

  @Override
  public EntityBatch<K, E> newBatch() {
    return clientTemplate.createBatch(entityMapper);
  }

  /**
   * Get the HBaseClientTemplate instance this DAO is using to interact with
   * HBase.
   * 
   * @return The HBaseClientTemplate instance.
   */
  public HBaseClientTemplate getHBaseClientTemplate() {
    return clientTemplate;
  }

  @Override
  public KeySchema getKeySchema() {
    return entityMapper.getKeySchema();
  }

  @Override
  public EntitySchema getEntitySchema() {
    return entityMapper.getEntitySchema();
  }

  /**
   * Gets the key serde for this DAO
   *
   * @return The KeySerDe
   */
  public KeySerDe<K> getKeySerDe() {
    return entityMapper.getKeySerDe();
  }

  /**
   * Gets the entity serde for this DAO
   *
   * @return The EntitySerDe
   */
  public EntitySerDe<E> getEntitySerDe() {
    return entityMapper.getEntitySerDe();
  }

  /**
   * Gets the EntityMapper for this DAO.
   *
   * @return EntityMapper
   */
  public EntityMapper<K, E> getEntityMapper() {
    return this.entityMapper;
  }

  public String getTableName() {
    return tableName;
  }

}
