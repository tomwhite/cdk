package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.MapDataset;
import com.cloudera.cdk.data.MapDatasetAccessor;
import com.cloudera.cdk.data.MapDatasetWriter;
import com.cloudera.cdk.data.MapEntry;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.avro.entities.ArrayRecord;
import com.cloudera.cdk.data.hbase.avro.entities.EmbeddedRecord;
import com.cloudera.cdk.data.hbase.avro.entities.TestEnum;
import com.cloudera.cdk.data.hbase.avro.entities.TestKey;
import com.cloudera.cdk.data.hbase.avro.entities.TestRecord;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HBaseDatasetRepositoryTest {

  private static final String keyString;
  private static final String testRecord;
  private static final String testGenericRecord;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";

  private HTablePool tablePool;

  static {
    try {
      keyString = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestKey.avsc"));
      testRecord = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestRecord.avsc"));
      testGenericRecord = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestGenericRecord.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @Before
  public void before() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
  }

  @After
  public void after() throws Exception {
    tablePool.close();
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testGeneric() throws Exception {

    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .keySchema(keyString)
        .schema(testGenericRecord)
        .get();
    MapDataset ds = repo.create(tableName, descriptor);
    MapDatasetAccessor<GenericRecord, GenericRecord> accessor = ds.newMapAccessor();

    // Create the new entities
    accessor.put(createGenericKey(0), createGenericEntity(0));
    accessor.put(createGenericKey(1), createGenericEntity(1));

    MapDatasetWriter<GenericRecord, GenericRecord> writer = ds.newMapWriter();
    writer.open();
    try {
      for (int i = 2; i < 10; ++i) {
        GenericRecord keyRecord = createGenericKey(i);
        GenericRecord entity = createGenericEntity(i);
        writer.write(keyRecord, entity);
      }
    } finally {
      writer.close();
    }

    // reload
    ds = repo.load(tableName);
    accessor = ds.newMapAccessor();

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      compareEntitiesWithUtf8(i, accessor.get(createGenericKey(i)));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<MapEntry<GenericRecord, GenericRecord>> reader = ds.newMapReader();
    reader.open();
    try {
      for (MapEntry<GenericRecord, GenericRecord> entry : reader) {
        compareEntitiesWithUtf8(cnt, entry.getEntity());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
    }

    GenericRecord key = createGenericKey(5);

    // test increment
    long incrementResult = accessor.increment(key, "increment", 5);
    assertEquals(15L, incrementResult);
    assertEquals(15L, ((Long) accessor.get(key).get("increment")).longValue());

    // test delete
    accessor.delete(key);
    GenericRecord deletedRecord = accessor.get(key);
    assertNull(deletedRecord);

  }

  @Test
  public void testSpecific() throws Exception {
    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .keySchema(keyString)
        .schema(testRecord)
        .get();
    MapDataset ds = repo.create(tableName, descriptor);
    MapDatasetAccessor<TestKey, TestRecord> accessor = ds.newMapAccessor();

    // Create the new entities
    accessor.put(createSpecificKey(0), createSpecificEntity(0));
    accessor.put(createSpecificKey(1), createSpecificEntity(1));

    MapDatasetWriter<TestKey, TestRecord> writer = ds.newMapWriter();
    writer.open();
    try {
      for (int i = 2; i < 10; ++i) {
        TestKey keyRecord = createSpecificKey(i);
        TestRecord entity = createSpecificEntity(i);
        writer.write(keyRecord, entity);
      }
    } finally {
      writer.close();
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      String iStr = Long.toString(i);
      compareEntitiesWithString(i, accessor.get(createSpecificKey(i)));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<MapEntry<TestKey, TestRecord>> reader = ds.newMapReader();
    reader.open();
    try {
      for (MapEntry<TestKey, TestRecord> entry : reader) {
        compareEntitiesWithString(cnt, entry.getEntity());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
    }

    TestKey key = createSpecificKey(5);

    // test increment
    long incrementResult = accessor.increment(key, "increment", 5);
    assertEquals(15L, incrementResult);
    assertEquals(15L, accessor.get(key).getIncrement().longValue());

    // test delete
    accessor.delete(key);
    TestRecord deletedRecord = accessor.get(key);
    assertNull(deletedRecord);
  }

  // TODO: remove duplication from ManagedDaoTest

  private GenericRecord createGenericKey(long uniqueIdx) {
    String iStr = Long.toString(uniqueIdx);
    Schema.Parser parser = new Schema.Parser();
    GenericRecord keyRecord = new GenericData.Record(parser.parse(keyString));
    keyRecord.put("part1", "part1_" + iStr);
    keyRecord.put("part2", "part2_" + iStr);
    return keyRecord;
  }

  private GenericRecord createGenericEntity(long uniqueIdx) {
    return createGenericEntity(uniqueIdx, testRecord);
  }

  /**
   * Creates a generic entity with the given schema, which must be migratable
   * from TestRecord. Data from TestRecord is filled in.
   *
   * @param uniqueIdx
   * @param schemaString
   * @return
   */
  private GenericRecord createGenericEntity(long uniqueIdx, String schemaString) {
    String iStr = Long.toString(uniqueIdx);

    Schema.Parser parser = new Schema.Parser();
    Schema entitySchema = parser.parse(schemaString);
    Schema embeddedSchema = entitySchema.getField("field4").schema();
    Schema arrayValueSchema = entitySchema.getField("field5").schema()
        .getElementType();

    GenericRecord entity = new GenericData.Record(entitySchema);
    entity.put("field1", "field1_" + iStr);
    entity.put("field2", "field2_" + iStr);
    entity.put("enum", "ENUM3");
    Map<CharSequence, CharSequence> field3Map = new HashMap<CharSequence, CharSequence>();
    field3Map.put("field3_key_1_" + iStr, "field3_value_1_" + iStr);
    field3Map.put("field3_key_2_" + iStr, "field3_value_2_" + iStr);
    entity.put("field3", field3Map);
    GenericRecord embedded = new GenericData.Record(embeddedSchema);
    embedded.put("embeddedField1", "embedded1");
    embedded.put("embeddedField2", 2L);
    entity.put("field4", embedded);

    List<GenericRecord> arrayRecordList = new ArrayList<GenericRecord>();
    GenericRecord subRecord = new GenericData.Record(arrayValueSchema);
    subRecord.put("subfield1", "subfield1");
    subRecord.put("subfield2", 1L);
    subRecord.put("subfield3", "subfield3");
    arrayRecordList.add(subRecord);

    subRecord = new GenericData.Record(arrayValueSchema);
    subRecord.put("subfield1", "subfield4");
    subRecord.put("subfield2", 1L);
    subRecord.put("subfield3", "subfield6");
    arrayRecordList.add(subRecord);

    entity.put("field5", arrayRecordList);

    entity.put("increment", 10L);

    return entity;
  }

  @SuppressWarnings("unchecked")
  private void compareEntitiesWithString(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);
    assertEquals("field1_" + iStr, record.get(0));
    assertEquals("field2_" + iStr, record.get(1));
    assertEquals(TestEnum.ENUM3.toString(), record.get(2).toString());
    assertEquals(
        "field3_value_1_" + iStr,
        ((Map<CharSequence, CharSequence>) record.get(3)).get("field3_key_1_"
            + iStr));
    assertEquals(
        "field3_value_2_" + iStr,
        ((Map<CharSequence, CharSequence>) record.get(3)).get("field3_key_2_"
            + iStr));
    assertEquals("embedded1", ((IndexedRecord) record.get(4)).get(0));
    assertEquals(2L, ((IndexedRecord) record.get(4)).get(1));

    assertEquals(2, ((List<?>) record.get(5)).size());
    assertEquals("subfield1",
        ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(1));
    assertEquals("subfield3",
        ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(2));
    assertEquals("subfield4",
        ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(1));
    assertEquals("subfield6",
        ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(2));
  }

  @SuppressWarnings("unchecked")
  private void compareEntitiesWithUtf8(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);
    assertEquals(new Utf8("field1_" + iStr), record.get(0));
    assertEquals(new Utf8("field2_" + iStr), record.get(1));
    assertEquals(TestEnum.ENUM3.toString(), record.get(2).toString());
    assertEquals(new Utf8("field3_value_1_" + iStr),
        ((Map<CharSequence, CharSequence>) record.get(3)).get(new Utf8(
            "field3_key_1_" + iStr)));
    assertEquals(new Utf8("field3_value_2_" + iStr),
        ((Map<CharSequence, CharSequence>) record.get(3)).get(new Utf8(
            "field3_key_2_" + iStr)));
    assertEquals(new Utf8("embedded1"), ((IndexedRecord) record.get(4)).get(0));
    assertEquals(2L, ((IndexedRecord) record.get(4)).get(1));

    assertEquals(2, ((List<?>) record.get(5)).size());
    assertEquals(new Utf8("subfield1"),
        ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(1));
    assertEquals(new Utf8("subfield3"),
        ((IndexedRecord) ((List<?>) record.get(5)).get(0)).get(2));
    assertEquals(new Utf8("subfield4"),
        ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(1));
    assertEquals(new Utf8("subfield6"),
        ((IndexedRecord) ((List<?>) record.get(5)).get(1)).get(2));
  }

  private TestKey createSpecificKey(long uniqueIdx) {
    String iStr = Long.toString(uniqueIdx);
    TestKey keyRecord = TestKey.newBuilder().setPart1("part1_" + iStr)
        .setPart2("part2_" + iStr).build();
    return keyRecord;
  }

  private TestRecord createSpecificEntity(long uniqueIdx) {
    String iStr = Long.toString(uniqueIdx);

    Map<String, String> field3Map = new HashMap<String, String>();
    field3Map.put("field3_key_1_" + iStr, "field3_value_1_" + iStr);
    field3Map.put("field3_key_2_" + iStr, "field3_value_2_" + iStr);

    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2L).build();

    List<ArrayRecord> arrayRecordList = new ArrayList<ArrayRecord>(2);
    ArrayRecord subRecord = ArrayRecord.newBuilder().setSubfield1("subfield1")
        .setSubfield2(1L).setSubfield3("subfield3").build();
    arrayRecordList.add(subRecord);
    subRecord = ArrayRecord.newBuilder().setSubfield1("subfield4")
        .setSubfield2(1L).setSubfield3("subfield6").build();
    arrayRecordList.add(subRecord);

    TestRecord entity = TestRecord.newBuilder().setField1("field1_" + iStr)
        .setField2("field2_" + iStr).setEnum$(TestEnum.ENUM3)
        .setField3(field3Map).setField4(embeddedRecord)
        .setField5(arrayRecordList).setIncrement(10L).build();
    return entity;
  }
}
