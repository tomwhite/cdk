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

import java.util.ArrayList;
import java.util.List;

public class MapKey<K> {

  private final List<KeyPartNameValue> partList = new ArrayList<KeyPartNameValue>();

  private MapKey(Builder<K> builder) {
    partList.addAll(builder.partList);
  }

  /**
   * Get the list of KeyPartNameValues.
   *
   * @return The list of KeyPartNameValues that make this key.
   */
  public List<KeyPartNameValue> getPartList() {
    return partList;
  }

  /**
   * Get a part of the key by its name.
   *
   * @param name
   *          The name of the key part to get, or null if it doesn't exist.
   * @return The KeyPartNameValue
   */
  public KeyPartNameValue getKeyPartByName(String name) {
    for (KeyPartNameValue value : partList) {
      if (name.equals(value.getName())) {
        return value;
      }
    }
    return null;
  }

  public static class Builder<K> {

    private final List<KeyPartNameValue> partList = new ArrayList<KeyPartNameValue>();

    public MapKey<K> build() {
      return new MapKey<K>(this);
    }

    public Builder<K> addKeyPart(String partName, Object value) {
      partList.add(new KeyPartNameValue(partName, value));
      return this;
    }
  }

  public static class KeyPartNameValue {

    private final String name;
    private final Object value;

    public KeyPartNameValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }
  }
}
