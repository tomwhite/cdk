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

import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.spi.AbstractRangeView;
import com.cloudera.cdk.data.spi.MarkerRange;

class DaoView<E> extends AbstractRangeView<E> {

  private final DaoDataset<E> dataset;

  DaoView(DaoDataset<E> dataset) {
    super(dataset);
    this.dataset = dataset;
  }

  private DaoView(DaoView<E> view, MarkerRange range) {
    super(view, range);
    this.dataset = view.dataset;
  }

  @Override
  protected DaoView<E> newLimitedCopy(MarkerRange newRange) {
    return new DaoView<E>(this, newRange);
  }

  @Override
  public DatasetReader<E> newReader() {
    return dataset.getDao().getScanner(toPartitionKey(range.getStart()),
        toPartitionKey(range.getEnd()));
  }

  @Override
  public DatasetWriter<E> newWriter() {
    // TODO: need to check keys are within range
    return dataset.getDao().newBatch();
  }

  @Override
  public DatasetAccessor<E> newAccessor() {
    // TODO: need to respect start and end of range
    return dataset.getDao();
  }

  @Override
  public Iterable<View<E>> getCoveringPartitions() {
    // TODO: use HBase InputFormat to construct splits
    throw new UnsupportedOperationException("getCoveringPartitions is not yet " +
        "supported.");
  }

  private PartitionKey toPartitionKey(MarkerRange.Boundary boundary) {
    if (boundary == null || boundary.getBound() == null) {
      return null;
    }
    // TODO: check inclusive/exclusive
    return dataset.getDescriptor().getPartitionStrategy().keyFor(boundary.getBound());
  }
}
