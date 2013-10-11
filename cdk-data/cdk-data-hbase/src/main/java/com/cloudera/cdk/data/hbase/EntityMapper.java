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

import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.KeySchema;

/**
 * An interface for mapping HBase Result instances to a Key/Entity pair, and a
 * Key/Entity to an HBase Put instances.
 * 
 * EntityMapper instances should be state-less so they can be reused across
 * multiple Result and Entity instances. They should encapsulate in one place
 * the mapping of business entities to and from HBase.
 * 
 * @param <K>
 *          The underlying key record type.
 * @param <E>
 *          The entity type
 */
public interface EntityMapper<E> {

  /**
   * Map an HBase Result instance to an Entity of type T. Retrieve the Key from
   * the result instance as well, and wraps both in an KeyEntity instance. This
   * KeyEntity instance is returned.
   * 
   * @param result
   *          The HBase result instance representing a row from an HBase table.
   * @return A KeyEntity instance which wraps a Key and an Entity of type T.
   */
  public E mapToEntity(Result result);

  /**
   * Map a Key and an entity of type T to an HBase Put instance.
   * 
   * @param key
   *          a key to use to construct the Put instance.
   * @param entity
   *          The entity which this function will map to a Put instance.
   * @return An HBase Put.
   */
  public PutAction mapFromEntity(E entity);

  /**
   * Maps a Key, fieldName and an increment value to an HBase Increment instance
   * that will increment the value in the cell pointed to by fieldName.
   * 
   * @param key
   *          a key to use to construct the Increment instance.
   * @param fieldName
   *          The name of the field we are incrementing
   * @param amount
   *          The amount to increment the field by
   * @return An HBase Increment
   */
  public Increment mapToIncrement(PartitionKey key, String fieldName, long amount);

  /**
   * Maps the result of an increment to the new value of the field that was
   * incremented.
   * 
   * @param result
   *          The HBase client Result object that contains the increment result
   * @param fieldName
   *          The name of the field we are getting the increment result for
   * @return The new field value.
   */
  public long mapFromIncrementResult(Result result, String fieldName);

  /**
   * Gets the set of required HBase columns that we would expect to be in the
   * result.
   * 
   * @return The set of required columns.
   */
  public Set<String> getRequiredColumns();

  /**
   * Gets the set of required column families that must exist in the HBase table
   * we would be mapping from.
   * 
   * @return The set of required column families.
   */
  public Set<String> getRequiredColumnFamilies();

  /**
   * Gets the key schema instance for this entity mapper.
   * 
   * @return The HBaseCommonKeySchema instance.
   */
  public KeySchema getKeySchema();

  /**
   * Gets the entity schema instance for this entity mapper.
   * 
   * @return The HBaseCommonEntitySchema instance.
   */
  public EntitySchema getEntitySchema();
  
  /**
   * Gets the key serde instance for this entity mapper
   * 
   * @return The key serde for the entity mapper
   */
  public KeySerDe getKeySerDe();

  /**
   * Gets the entity serde instance for this entity mapper
   * 
   * @return The entity serde for the entity mapper
   */
  public EntitySerDe<E> getEntitySerDe();
}
