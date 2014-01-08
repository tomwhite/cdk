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

package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetException;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.spi.Key;
import com.cloudera.cdk.data.spi.RangePredicate;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class FileSystemPartitionIterator implements Iterator<Key>, Iterable<Key> {

  private static final Logger logger = LoggerFactory.getLogger(
      FileSystemPartitionIterator.class);

  private final FileSystem fs;
  private final Path rootDirectory;
  private final Iterator<Key> iterator;

  private static class KeyPredicate implements Predicate<Key> {
    private final RangePredicate predicate;

    public KeyPredicate(RangePredicate predicate) {
      this.predicate = predicate;
    }

    @Override public boolean apply(Key key) {
      return predicate.apply(key);
    }
  }

  class FileSystemIterator extends MultiLevelIterator<String> {
    public FileSystemIterator(int depth) throws IOException {
      super(depth);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<String> getLevel(List<String> current) {
      final Set<String> dirs = Sets.newLinkedHashSet();

      Path dir = rootDirectory;
      for (int i = 0, n = current.size(); i < n; i += 1) {
        dir = new Path(dir, current.get(i));
      }

      try {
        for (FileStatus stat : fs.listStatus(dir, PathFilters.notHidden())) {
          if (stat.isDirectory()) {
            // TODO: add a check here for range.couldContain(Marker)
            dirs.add(stat.getPath().getName());
          }
        }
      } catch (IOException ex) {
        throw new DatasetException("Cannot list directory:" + dir, ex);
      }

      return dirs;
    }
  }

  /**
   * Conversion function to transform a List into a {@link Key}.
   */
  private static class MakeKey implements Function<List<String>, Key> {
    private final List<FieldPartitioner> partitioners;
    private final Key reusableKey;
    private final PathConversion convert;

    public MakeKey(PartitionStrategy strategy) {
      this.partitioners = strategy.getFieldPartitioners();
      this.reusableKey = new Key(strategy);
      this.convert = new PathConversion();
    }

    @Override
    @SuppressWarnings("unchecked")
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification="Non-null @Nullable parameter inherited from Function")
    public Key apply(List<String> dirs) {
      List<Object> values = Lists.newArrayListWithCapacity(dirs.size());
      for (int i = 0, n = partitioners.size(); i < n; i += 1) {
        values.add(convert.valueForDirname(partitioners.get(i), dirs.get(i)));
      }
      reusableKey.replaceValues(values);
      return reusableKey;
    }
  }

  FileSystemPartitionIterator(
      FileSystem fs, Path root, PartitionStrategy strategy,
      final RangePredicate predicate)
      throws IOException {
    Preconditions.checkArgument(fs.isDirectory(root));
    this.fs = fs;
    this.rootDirectory = root;
    this.iterator = Iterators.filter(
        Iterators.transform(
            new FileSystemIterator(strategy.getFieldPartitioners().size()),
            new MakeKey(strategy)),
        new KeyPredicate(predicate));
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Key next() {
    return iterator.next();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public Iterator<Key> iterator() {
    return this;
  }


}
