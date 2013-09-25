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
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import java.util.Iterator;

class SpecificAvroDaoDataset implements MapDataset {
  private SpecificAvroDao dao;
  private DatasetDescriptor descriptor;

  public SpecificAvroDaoDataset(SpecificAvroDao dao, DatasetDescriptor descriptor) {
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
  public <K, E> MapDatasetAccessor<K, E> newMapAccessor() {
    return dao;
  }

  @Override
  public <K, E> MapDatasetWriter<K, E> getMapWriter() {
    return new SpecificAvroDaoDatasetWriter(dao.newBatch());
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader() {
    return new SpecificAvroDaoDatasetReader(dao.getScanner());
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader(K startKey, K stopKey) {
    return new SpecificAvroDaoDatasetReader(dao.getScanner(startKey, stopKey));
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader(MapKey startKey,
      MapKey stopKey) {
    return new SpecificAvroDaoDatasetReader(dao.getScanner(GenericAvroDaoDataset
        .toPartialKey(startKey), GenericAvroDaoDataset.toPartialKey(stopKey)));
  }

  private class SpecificAvroDaoDatasetReader<K, E> extends AbstractDatasetReader<MapEntry<K, E>> {

    private EntityScanner<K, E> scanner;
    private Iterator<KeyEntity<K, E>> iterator;

    public SpecificAvroDaoDatasetReader(EntityScanner<K, E> scanner) {
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

  private class SpecificAvroDaoDatasetWriter<K, E> implements MapDatasetWriter<K, E> {

    private EntityBatch batch;

    public SpecificAvroDaoDatasetWriter(EntityBatch batch) {
      this.batch = batch;
    }

    @Override
    public void open() {
      // noop
    }

    @Override
    public void write(K key, E entity) {
      batch.put(key, entity);
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
