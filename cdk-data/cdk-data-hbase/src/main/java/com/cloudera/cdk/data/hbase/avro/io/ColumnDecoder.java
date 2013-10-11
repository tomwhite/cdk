// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

/**
 * An Avro Decoder implementation used for decoding Avro instances from HBase
 * columns. This is basically an Avro BinaryDecoder with custom encoding of int,
 * long, and String types.
 * 
 * int and long are serialized in standard 4 and 8 byte format (instead of
 * Avro's ZigZag encoding) so that we can use HBase's atomic increment
 * functionality on columns.
 * 
 * Strings are encoding as UTF-8 bytes. This is for backward compatibility
 * reasons, and is something we want to change in the future.
 */
public class ColumnDecoder extends Decoder {

  private final BinaryDecoder wrappedDecoder;
  private final InputStream in;

  public ColumnDecoder(InputStream in) {
    this.in = in;
    this.wrappedDecoder = new DecoderFactory().binaryDecoder(in, null);
  }

  @Override
  public void readNull() throws IOException {
    wrappedDecoder.readNull();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return wrappedDecoder.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    DataInputStream dataIn = new DataInputStream(in);
    return dataIn.readInt();
  }

  @Override
  public long readLong() throws IOException {
    DataInputStream dataIn = new DataInputStream(in);
    return dataIn.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return wrappedDecoder.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return wrappedDecoder.readDouble();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    int bytesAvailable = in.available();
    byte[] bytes = new byte[bytesAvailable];
    in.read(bytes);
    String s = new String(bytes);
    return new Utf8(s);
  }

  @Override
  public String readString() throws IOException {
    return readString(null).toString();
  }

  @Override
  public void skipString() throws IOException {
    int bytesAvailable = in.available();
    in.skip(bytesAvailable);
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return wrappedDecoder.readBytes(old);
  }

  @Override
  public void skipBytes() throws IOException {
    wrappedDecoder.skipBytes();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    wrappedDecoder.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    wrappedDecoder.skipFixed(length);
  }

  @Override
  public int readEnum() throws IOException {
    return wrappedDecoder.readEnum();
  }

  @Override
  public long readArrayStart() throws IOException {
    return wrappedDecoder.readArrayStart();
  }

  @Override
  public long arrayNext() throws IOException {
    return wrappedDecoder.arrayNext();
  }

  @Override
  public long skipArray() throws IOException {
    return wrappedDecoder.skipArray();
  }

  @Override
  public long readMapStart() throws IOException {
    return wrappedDecoder.readMapStart();
  }

  @Override
  public long mapNext() throws IOException {
    return wrappedDecoder.mapNext();
  }

  @Override
  public long skipMap() throws IOException {
    return wrappedDecoder.skipMap();
  }

  @Override
  public int readIndex() throws IOException {
    return wrappedDecoder.readIndex();
  }

}
