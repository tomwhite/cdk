/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.View;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

/**
 * A common View base class to simplify implementations of Views created from ranges.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 0.9.0
 */
@Immutable
public abstract class AbstractRangeView<E> implements View<E> {

  protected final Dataset<E> dataset;
  protected final MarkerRange range;

  // This class is Immutable and must be thread-safe
  protected final ThreadLocal<CompleteKey> keys;

  protected AbstractRangeView(Dataset<E> dataset) {
    this.dataset = dataset;
    final DatasetDescriptor descriptor = dataset.getDescriptor();
    if (descriptor.isPartitioned()) {
      this.range = new MarkerRange(new MarkerComparator(
          descriptor.getPartitionStrategy()));
      this.keys = new ThreadLocal<CompleteKey>() {
        @Override
        protected CompleteKey initialValue() {
          return new CompleteKey(descriptor.getPartitionStrategy());
        }
      };
    } else {
      // use UNDEFINED, which handles inappropriate calls to range methods
      this.range = MarkerRange.UNDEFINED;
      this.keys = null; // not used
    }
  }

  protected AbstractRangeView(AbstractRangeView<E> view, MarkerRange range) {
    this.dataset = view.dataset;
    this.range = range;
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
  }

  protected abstract AbstractRangeView<E> newLimitedCopy(MarkerRange subRange);

  @Override
  public Dataset<E> getDataset() {
    return dataset;
  }

  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support deletion");
  }

  public boolean contains(E entity) {
    if (dataset.getDescriptor().isPartitioned()) {
      return range.contains(keys.get().reuseFor(entity));
    } else {
      return true;
    }
  }

  public boolean contains(Marker marker) {
    return range.contains(marker);
  }

  public View<E> from(Marker start) {
    return newLimitedCopy(range.from(start));
  }

  public View<E> fromAfter(Marker start) {
    return newLimitedCopy(range.fromAfter(start));
  }

  public View<E> to(Marker end) {
    return newLimitedCopy(range.to(end));
  }

  public View<E> toBefore(Marker end) {
    return newLimitedCopy(range.toBefore(end));
  }

  public View<E> of(Marker partial) {
    return newLimitedCopy(range.of(partial));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o == null) || !Objects.equal(this.getClass(), o.getClass())) {
      return false;
    }

    AbstractRangeView that = (AbstractRangeView) o;
    return (Objects.equal(this.dataset, that.dataset) &&
        Objects.equal(this.range, that.range));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), dataset, range);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dataset", dataset)
        .add("range", range)
        .toString();
  }
}
