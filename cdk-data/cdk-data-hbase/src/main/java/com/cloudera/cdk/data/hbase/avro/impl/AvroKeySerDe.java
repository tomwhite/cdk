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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.hbase.KeySerDe;
import com.cloudera.cdk.data.hbase.avro.io.MemcmpDecoder;
import com.cloudera.cdk.data.hbase.avro.io.MemcmpEncoder;

/**
 * Avro implementation of the KeySerDe interface. This will serialize Keys and
 * PartialKeys to a special ordered memcmp-able avro encoding.
 * 
 * @param <K>
 *          The Key type.
 */
public class AvroKeySerDe implements KeySerDe {

  private final Schema schema;
  private final Schema[] partialSchemas;
  private final PartitionStrategy partitionStrategy;

  public AvroKeySerDe(Schema schema, PartitionStrategy partitionStrategy) {
    this.schema = schema;
    int fieldSize = schema.getFields().size();
    partialSchemas = new Schema[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      if (i == (fieldSize - 1)) {
        break;
      }
      List<Field> partialFieldList = new ArrayList<Field>();
      for (Field field : schema.getFields().subList(0, i + 1)) {
        partialFieldList.add(AvroUtils.cloneField(field));
      }
      partialSchemas[i] = Schema.createRecord(partialFieldList);
    }
    this.partitionStrategy = partitionStrategy;
  }

  @Override
  public byte[] serialize(PartitionKey key) {
    if (key.getLength() == 0) {
      return new byte[0];
    }
    
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new MemcmpEncoder(outputStream);
    
    Schema schemaToUse;
    if (key.getLength() == schema.getFields().size()) {
      schemaToUse = schema;
    } else {
      schemaToUse = partialSchemas[key.getLength()-1];
    }
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schemaToUse);
    GenericRecord record = new GenericData.Record(schemaToUse);
    for (int i = 0; i < key.getLength(); i++) {
      record.put(i, key.get(i));
    }
    AvroUtils.writeAvroEntity(record, encoder, datumWriter);
    return outputStream.toByteArray();
  }

  @Override
  public PartitionKey deserialize(byte[] keyBytes) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(keyBytes);
    Decoder decoder = new MemcmpDecoder(inputStream);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
        schema);
    GenericRecord genericRecord = AvroUtils
        .readAvroEntity(decoder, datumReader);

    Object[] keyParts = new Object[genericRecord.getSchema().getFields().size()];
    for (int i = 0; i < genericRecord.getSchema().getFields().size(); i++) {
      keyParts[i] = genericRecord.get(i);
    }
    return partitionStrategy.partitionKey(keyParts);
  }

  @Override
  public byte[] serialize(Object... keyPartValues) {
    return serialize(partitionStrategy.partitionKey(keyPartValues));
  }
}
