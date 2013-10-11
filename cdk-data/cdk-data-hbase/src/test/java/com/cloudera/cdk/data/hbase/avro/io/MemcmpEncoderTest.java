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
package com.cloudera.cdk.data.hbase.avro.io;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;

import org.apache.avro.io.Encoder;
import org.junit.Before;
import org.junit.Test;

public class MemcmpEncoderTest {

  private ByteArrayOutputStream byteOutputStream;
  private Encoder encoder;

  @Before
  public void setUp() {
    byteOutputStream = new ByteArrayOutputStream();
    encoder = new MemcmpEncoder(byteOutputStream);
  }

  @Test
  public void testEncodeInt() throws Exception {
    encoder.writeInt(1);
    assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x00, (byte) 0x00,
        (byte) 0x01 }, byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeInt(-1);
    assertArrayEquals(new byte[] { (byte) 0x7f, (byte) 0xff, (byte) 0xff,
        (byte) 0xff }, byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeInt(0);
    assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x00, (byte) 0x00,
        (byte) 0x00 }, byteOutputStream.toByteArray());
  }

  @Test
  public void testEncodeLong() throws Exception {
    encoder.writeLong(1L);
    assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01 },
        byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeLong(-1L);
    assertArrayEquals(new byte[] { (byte) 0x7f, (byte) 0xff, (byte) 0xff,
        (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
        byteOutputStream.toByteArray());
    byteOutputStream.reset();
    encoder.writeLong(0L);
    assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 },
        byteOutputStream.toByteArray());
  }

  @Test
  public void testWriteBytes() throws Exception {
    encoder.writeBytes(new byte[] { (byte) 0x01, (byte) 0x00, (byte) 0xff }, 0,
        3);
    assertArrayEquals(new byte[] { (byte) 0x01, (byte) 0x00, (byte) 0x01,
        (byte) 0xff, (byte) 0x00, (byte) 0x00 }, byteOutputStream.toByteArray());
  }
}
