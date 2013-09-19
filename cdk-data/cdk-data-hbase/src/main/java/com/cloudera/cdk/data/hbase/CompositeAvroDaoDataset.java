package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.FieldPartitioner;
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

class CompositeAvroDaoDataset implements Dataset {
  private Dao<SpecificRecord, Map<String, SpecificRecord>> dao;
  private DatasetDescriptor descriptor;
  private Schema keySchema;


  public CompositeAvroDaoDataset(Dao<SpecificRecord, Map<String, SpecificRecord>> dao, DatasetDescriptor descriptor) {
    this.dao = dao;
    this.descriptor = descriptor;
    this.keySchema = HBaseMetadataProvider.getKeySchema(descriptor);
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
  public <E> DatasetAccessor<E> newAccessor() {
    return new DatasetAccessor<E>() {
      @Override
      public E get(PartitionKey key) {
        return (E) dao.get(toSpecificRecord(key));
      }

      @Override
      public boolean put(E e) {
        // pull out one of the composite fields to act as a key
        // TODO: check that all composite fields have the same key
        Map<String, SpecificRecord> composite = (Map<String, SpecificRecord>) e;
        SpecificRecord key = composite.values().iterator().next();
        return dao.put(key, (Map<String, SpecificRecord>) e);
      }

      @Override
      public long increment(PartitionKey key, String fieldName, long amount) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void delete(PartitionKey key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean delete(PartitionKey key, E entity) {
        throw new UnsupportedOperationException();
      }

      private SpecificRecord toSpecificRecord(PartitionKey key) {
        SpecificGenericRecord keyRecord = new SpecificGenericRecord(keySchema);
        int i = 0;
        for (FieldPartitioner fp : descriptor.getPartitionStrategy().getFieldPartitioners()) {
          keyRecord.put(fp.getName(), key.get(i++));
        }
        return keyRecord;
      }
    };
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