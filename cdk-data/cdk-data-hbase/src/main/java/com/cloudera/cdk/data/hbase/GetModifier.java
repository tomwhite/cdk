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

import org.apache.hadoop.hbase.client.Get;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Get instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface GetModifier {

  /**
   * Modify the Get instance.
   * 
   * @param get
   *          The Get instance to modify.
   * @return The modified Get instance
   */
  public Get modifyGet(Get get);
}
