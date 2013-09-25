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
import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;

class CompositeAvroDaoDataset implements MapDataset {
  private Dao<SpecificRecord, Map<String, SpecificRecord>> dao;
  private DatasetDescriptor descriptor;

  public CompositeAvroDaoDataset(Dao<SpecificRecord, Map<String, SpecificRecord>> dao, DatasetDescriptor descriptor) {
    this.dao = dao;
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    if (dao instanceof BaseDao) {
      return ((BaseDao) dao).getTableName();
    }
    throw new IllegalArgumentException("Name not known"); // TODO: move name to Dao
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
    return null;  //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public <K, E> MapDatasetWriter<K, E> getMapWriter() {
    return null;  //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader(K startKey, K stopKey) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <K, E> DatasetReader<MapEntry<K, E>> getMapReader(MapKey startKey,
      MapKey stopKey) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private class SpecificGenericRecord extends GenericData.Record implements SpecificRecord {
    public SpecificGenericRecord(Schema schema) {
      super(schema);
    }
  }

  private class SpecificAvroDaoDatasetReader<K, E> extends AbstractDatasetReader {

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
    public E next() {
      return iterator.next().getEntity();
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

  private class SpecificAvroDaoDatasetWriter<E> implements DatasetWriter<E> {

    private EntityBatch batch;

    public SpecificAvroDaoDatasetWriter(EntityBatch batch) {
      this.batch = batch;
    }

    @Override
    public void open() {
      // noop
    }

    @Override
    public void write(E e) {
      // the entity contains the key fields so we can use the same GenericRecord
      // instance as a key
      batch.put(e, e);
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
