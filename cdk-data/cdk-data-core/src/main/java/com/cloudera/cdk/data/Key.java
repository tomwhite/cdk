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
package com.cloudera.cdk.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * A key for retrieving entities from a {@link RandomAccessDataset}.
 * </p>
 */
public class Key {

  private final Map<String, Object> values;

  Key(Map<String, Object> values) {
    this.values = values;
  }

  public boolean has(String name) {
    return values.containsKey(name);
  }

  public Object get(String name) {
    return values.get(name);
  }

  public static class Builder {

    private Set<String> fieldNames;
    private final Map<String, Object> values;

    public Builder(RandomAccessDataset dataset) {
      this.fieldNames = Sets.newHashSet();
      for (FieldPartitioner fp : dataset.getDescriptor().getPartitionStrategy().getFieldPartitioners()) {
        fieldNames.add(fp.getSourceName());
        fieldNames.add(fp.getName());
      }
      this.values = Maps.newHashMap();
    }

    public Builder add(String name, Object value) {
      Preconditions.checkArgument(fieldNames.contains(name), "Field %s not in schema.",
          name);
      values.put(name, value);
      return this;
    }

    public Key build() {
      return new Key(values);
    }
  }
}
