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
package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.MapDataset;
import com.cloudera.cdk.data.MapDatasetAccessor;
import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.avro.entities.CompositeRecord;
import com.cloudera.cdk.data.hbase.avro.entities.SubRecord1;
import com.cloudera.cdk.data.hbase.avro.entities.SubRecord2;
import com.cloudera.cdk.data.hbase.avro.entities.TestKey;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;
import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class CompositeDatasetTest {

  private static final String keyString;
  private static final String subRecord1String;
  private static final String subRecord2String;
  private static final String tableName = "testtable";
  private HTablePool tablePool;

  static {
    try {
      keyString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestKey.avsc"));
      subRecord1String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubRecord1.avsc"));
      subRecord2String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubRecord2.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    byte[][] cfNames = { Bytes.toBytes("meta"), Bytes.toBytes("conflict"),
        Bytes.toBytes("_s") };
    HBaseTestUtils.util.createTable(tableNameBytes, cfNames);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @Before
  public void beforeTest() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
  }

  @After
  public void afterTest() throws Exception {
    tablePool.close();
  }

  @Test
  public void testSpecific() throws Exception {

    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    // Create sub-datasets
    // TODO: should these be automatically created as a part of the composite dataset?
    repo.create(tableName, new DatasetDescriptor.Builder()
        .keySchema(keyString)
        .schema(subRecord1String)
        .get());
    repo.create(tableName, new DatasetDescriptor.Builder()
        .keySchema(keyString)
        .schema(subRecord2String)
        .get());

    // Create composite dataset
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .keySchema(keyString)
        .schema(CompositeRecord.SCHEMA$)
        .get();
    MapDataset ds = repo.create(tableName, descriptor);
    MapDatasetAccessor<TestKey, CompositeRecord> accessor = ds.newMapAccessor();

    // Construct records and keys
    TestKey testKey = TestKey.newBuilder().setPart1("1").setPart2("1").build();

    SubRecord1 subRecord1 = SubRecord1.newBuilder().setField1("field1_1")
        .setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setField1("field2_1")
        .setField2("field2_2").build();

    CompositeRecord compositeRecord = CompositeRecord.newBuilder()
        .setSubRecord1(subRecord1).setSubRecord2(subRecord2).build();

    // Test put and get
    accessor.put(testKey, compositeRecord);
    CompositeRecord returnedCompositeRecord = accessor.get(testKey);
    assertEquals("field1_1", returnedCompositeRecord.getSubRecord1()
        .getField1());
    assertEquals("field1_2", returnedCompositeRecord.getSubRecord1()
        .getField2());
    assertEquals("field2_1", returnedCompositeRecord.getSubRecord2()
        .getField1());
    assertEquals("field2_2", returnedCompositeRecord.getSubRecord2()
        .getField2());

    // Test OCC
    assertFalse(accessor.put(testKey, compositeRecord));
    assertTrue(accessor.put(testKey, returnedCompositeRecord));

    // Test null field
    testKey = TestKey.newBuilder().setPart1("1").setPart2("2").build();
    compositeRecord = CompositeRecord.newBuilder().setSubRecord1(subRecord1)
        .build();
    accessor.put(testKey, compositeRecord);
    compositeRecord = accessor.get(testKey);
    assertEquals(null, compositeRecord.getSubRecord2());
  }
}
