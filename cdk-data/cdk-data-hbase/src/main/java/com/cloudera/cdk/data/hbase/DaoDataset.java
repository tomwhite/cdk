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

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.spi.AbstractDataset;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DaoDataset<E> extends AbstractDataset<E> {

  private static final Logger logger = LoggerFactory
      .getLogger(DaoDataset.class);

  private String name;
  private Dao<E> dao;
  private DatasetDescriptor descriptor;
  private final DaoView<E> unbounded;

  public DaoDataset(String name, Dao<E> dao, DatasetDescriptor descriptor) {
    this.name = name;
    this.dao = dao;
    this.descriptor = descriptor;
    this.unbounded = new DaoView<E>(this);
  }

  Dao<E> getDao() {
    return dao;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Dataset<E> getPartition(PartitionKey key, boolean autoCreate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(PartitionKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Dataset<E>> getPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetWriter<E> newWriter() {
    logger.debug("Getting writer to dataset:{}", this);

    return unbounded.newWriter();
  }

  @Override
  public DatasetReader<E> newReader() {
    logger.debug("Getting reader for dataset:{}", this);

    return unbounded.newReader();
  }

  @Override
  public DatasetAccessor<E> newAccessor() {
    logger.debug("Getting accessor for dataset:{}", this);

    return unbounded.newAccessor();
  }

  @Override
  public Iterable<View<E>> getCoveringPartitions() {
    Preconditions.checkState(descriptor.isPartitioned(),
        "Attempt to get partitions on a non-partitioned dataset (name:%s)",
        name);

    return unbounded.getCoveringPartitions();
  }

  @Override
  public View<E> from(Marker start) {
    return unbounded.from(start);
  }

  @Override
  public View<E> fromAfter(Marker start) {
    return unbounded.fromAfter(start);
  }

  @Override
  public View<E> to(Marker end) {
    return unbounded.to(end);
  }

  @Override
  public View<E> toBefore(Marker end) {
    return unbounded.toBefore(end);
  }

  @Override
  public View<E> in(Marker partial) {
    return unbounded.in(partial);
  }

}
