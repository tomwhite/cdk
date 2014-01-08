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
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.spi.AbstractRangeView;
import com.cloudera.cdk.data.spi.Key;
import com.cloudera.cdk.data.spi.MarkerRange;

import com.cloudera.cdk.data.spi.RangePredicate;

class DaoView<E> extends AbstractRangeView<E> {

  private final DaoDataset<E> dataset;

  DaoView(DaoDataset<E> dataset) {
    super(dataset);
    this.dataset = dataset;
  }

  private DaoView(DaoView<E> view, RangePredicate p) {
    super(view, p);
    this.dataset = view.dataset;
  }

  @Override
  protected DaoView<E> filter(RangePredicate p) {
    return new DaoView<E>(this, p);
  }

  @Override
  public DatasetReader<E> newReader() {
    MarkerRange range = predicate.getRange();
    return dataset.getDao().getScanner(toPartitionKey(range.getStart()),
        range.getStart().isInclusive(), toPartitionKey(range.getEnd()),
        range.getEnd().isInclusive());
  }

  @Override
  public DatasetWriter<E> newWriter() {
    final DatasetWriter<E> wrappedWriter = dataset.getDao().newBatch();
    final Key partitionStratKey = new Key(dataset.getDescriptor().getPartitionStrategy());
    // Return a dataset writer that checks on write that an entity is within the
    // range of the view
    return new DatasetWriter<E>() {
      @Override
      public void open() {
        wrappedWriter.open();
      }

      @Override
      public void write(E entity) {
        Key key = partitionStratKey.reuseFor(entity);
        if (!predicate.apply(key)) {
          throw new IllegalArgumentException("View does not contain entity: "
              + entity);
        }
        wrappedWriter.write(entity);
      }

      @Override
      public void flush() {
        wrappedWriter.flush();
      }

      @Override
      public void close() {
        wrappedWriter.close();
      }

      @Override
      public boolean isOpen() {
        return wrappedWriter.isOpen();
      }
    };
  }

  @Override
  public Iterable<View<E>> getCoveringPartitions() {
    // TODO: use HBase InputFormat to construct splits
    throw new UnsupportedOperationException("getCoveringPartitions is not yet "
        + "supported.");
  }

  @SuppressWarnings("deprecation")
  private PartitionKey toPartitionKey(MarkerRange.Boundary boundary) {
    if (boundary == null || boundary.getBound() == null) {
      return null;
    }

    return DaoDataset.keyFor(dataset.getDescriptor().getPartitionStrategy(),
        boundary.getBound());
  }
}
