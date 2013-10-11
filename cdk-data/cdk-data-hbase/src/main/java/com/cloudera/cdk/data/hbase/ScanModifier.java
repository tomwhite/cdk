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

import org.apache.hadoop.hbase.client.Scan;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Scan instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface ScanModifier {

  /**
   * Modify the Scan instance.
   * 
   * @param scan
   *          The Scan instance to modify
   * @return The modified Scan instance
   */
  public Scan modifyScan(Scan scan);
}
