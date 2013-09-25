package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.MapDataset;
import com.cloudera.cdk.data.MapDatasetAccessor;
import com.cloudera.cdk.data.MapDatasetWriter;
import com.cloudera.cdk.data.MapEntry;
import com.cloudera.cdk.data.MapKey;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.dao.PartialKey;
import com.cloudera.cdk.data.hbase.avro.GenericAvroDao;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;

class GenericAvroDaoDataset implements MapDataset {

  private final GenericAvroDao dao;
  private DatasetDescriptor descriptor;

  public GenericAvroDaoDataset(GenericAvroDao dao, DatasetDescriptor descriptor) {
    this.dao = dao;
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    return dao.getTableName();
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Dataset getPartition(PartitionKey key, boolean autoCreate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(PartitionKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <E> DatasetWriter<E> getWriter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <E> DatasetReader<E> getReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Dataset> getPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MapDatasetAccessor<GenericRecord, GenericRecord> newMapAccessor() {
    return dao;
  }

  @Override
  public MapDatasetWriter<GenericRecord, GenericRecord> getMapWriter() {
    return new GenericAvroDaoDatasetWriter(dao.newBatch());
  }

  @Override
  public DatasetReader<MapEntry<GenericRecord, GenericRecord>> getMapReader() {
    return new GenericAvroDaoDatasetReader(dao.getScanner());
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader(K startKey, K stopKey) {
    return (DatasetReader<MapEntry<K, E>>) new GenericAvroDaoDatasetReader(
        dao.getScanner((GenericRecord) startKey, (GenericRecord) stopKey));
  }

  @Override
  public DatasetReader<MapEntry<GenericRecord, GenericRecord>> getMapReader(MapKey startKey,
      MapKey stopKey) {
    return new GenericAvroDaoDatasetReader(dao.getScanner(toPartialKey(startKey),
        toPartialKey(stopKey)));
  }

  static <K> PartialKey<K> toPartialKey(@Nullable MapKey<K> mapKey) {
    if (mapKey == null) {
      return null;
    }
    PartialKey.Builder<K> builder = new PartialKey.Builder<K>();
    for (MapKey.KeyPartNameValue part : mapKey.getPartList()) {
      builder.addKeyPart(part.getName(), part.getValue());
    }
    return builder.build();
  }

  private class GenericAvroDaoDatasetReader<K, E> extends AbstractDatasetReader<MapEntry<K, E>> {

    private EntityScanner<K, E> scanner;
    private Iterator<KeyEntity<K, E>> iterator;

    public GenericAvroDaoDatasetReader(EntityScanner<K, E> scanner) {
      this.scanner = scanner;
    }

    @Override
    public void open() {
      scanner.open();
      iterator = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public MapEntry<K, E> next() {
      KeyEntity<K, E> keyEntity = iterator.next();
      return new MapEntry<K, E>(keyEntity.getKey(), keyEntity.getEntity());
    }

    @Override
    public void close() {
      scanner.close();
    }

    @Override
    public boolean isOpen() {
      return true; // TODO
    }
  }

  private class GenericAvroDaoDatasetWriter implements MapDatasetWriter<GenericRecord, GenericRecord> {

    private EntityBatch<GenericRecord, GenericRecord> batch;

    public GenericAvroDaoDatasetWriter(EntityBatch<GenericRecord, GenericRecord> batch) {
      this.batch = batch;
    }

    @Override
    public void open() {
      // noop
    }

    @Override
    public void write(GenericRecord key, GenericRecord value) {
      batch.put(key, value);
    }

    @Override
    public void flush() {
      batch.flush();
    }

    @Override
    public void close() {
      batch.close();
    }

    @Override
    public boolean isOpen() {
      return true; // TODO
    }
  }

}
