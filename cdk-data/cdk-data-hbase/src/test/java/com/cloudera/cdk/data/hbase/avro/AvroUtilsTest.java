package com.cloudera.cdk.data.hbase.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

public class AvroUtilsTest {
  
  Schema.Parser parser = new Schema.Parser();
  
  @Test
  public void testReadAvroEntity() throws Exception {
    String schemaString = "{ \"type\": \"int\" }";
    InputStream is = new ByteArrayInputStream(schemaString.getBytes());
    Schema schema = parser.parse(is);
    byte[] bytes = new byte[] { (byte) 1 };
    DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema);
    Integer i = AvroUtils.readAvroEntity(bytes, reader);
    assertEquals(-1, i.intValue());
  }

  @Test
  public void testWriteAvroEntity() throws Exception {
    String schemaString = "{ \"type\": \"int\" }";
    InputStream is = new ByteArrayInputStream(schemaString.getBytes());
    Schema schema = parser.parse(is);
    DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
    byte[] bytes = AvroUtils.writeAvroEntity(1, writer);
    assertArrayEquals(new byte[] { (byte) 2 }, bytes);
  }
}
