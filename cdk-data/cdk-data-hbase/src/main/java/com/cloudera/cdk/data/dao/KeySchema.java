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
package com.cloudera.cdk.data.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.dao.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.partition.IdentityFieldPartitioner;

/**
 * The KeySchema type.
 */
public class KeySchema {

  private final String rawSchema;
  private final PartitionStrategy partitionStrategy;

  /**
   * @param rawSchema
   *          The raw schema
   */
  public KeySchema(String rawSchema, Collection<FieldMapping> fieldMappings) {
    this.rawSchema = rawSchema;
    List<FieldPartitioner> fieldPartitioners = new ArrayList<FieldPartitioner>();
    for (FieldMapping fieldMapping : fieldMappings) {
      IdentityFieldPartitioner fieldPartitioner = new IdentityFieldPartitioner(
          fieldMapping.getFieldName(), 1);
      fieldPartitioners.add(fieldPartitioner);
    }
    partitionStrategy = new PartitionStrategy(
        fieldPartitioners.toArray(new FieldPartitioner[0]));
  }
  
  public KeySchema(String rawSchema, PartitionStrategy partitionStrategy) {
    this.rawSchema = rawSchema;
    this.partitionStrategy = partitionStrategy;
  }

  /**
   * Get the raw schema
   * 
   * @return The raw schema.
   */
  public String getRawSchema() {
    return rawSchema;
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }
}
