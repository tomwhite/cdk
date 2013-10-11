// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.dao.HBaseClientException;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Static utility functions for working with the HBase API.
 */
public class HBaseUtils {

  /**
   * Interface for Internal HBase Operations
   */
  private static interface Operation {

    // Add a Column to an Operation
    void addColumn(byte[] family, byte[] column);

    // Add a Family to an Operation
    void addFamily(byte[] family);

  }

  /**
   * Given a list of puts, create a new put with the values in each put merged
   * together. It is expected that no puts have a value for the same fully
   * qualified column. Return the new put.
   * 
   * @param key
   *          The key of the new put.
   * @param putList
   *          The list of puts to merge
   * @return the new Put instance
   */
  public static Put mergePuts(byte[] keyBytes, List<Put> putList) {
    Put put = new Put(keyBytes);
    for (Put putToMerge : putList) {
      Map<byte[], List<KeyValue>> familyMap = putToMerge.getFamilyMap();
      for (List<KeyValue> keyValueList : familyMap.values()) {
        for (KeyValue keyValue : keyValueList) {
          try {
            put.add(keyValue);
          } catch (IOException e) {
            throw new HBaseClientException("Could not add KeyValue to put", e);
          }
        }
      }
    }
    return put;
  }

  /**
   * Given a list of PutActions, create a new PutAction with the values in each
   * put merged together. It is expected that no puts have a value for the same
   * fully qualified column. Return the new PutAction.
   * 
   * @param key
   *          The key of the new put.
   * @param putActionList
   *          The list of PutActions to merge
   * @return the new PutAction instance
   */
  public static PutAction mergePutActions(byte[] keyBytes,
      List<PutAction> putActionList) {
    VersionCheckAction checkAction = null;
    List<Put> putsToMerge = new ArrayList<Put>();
    for (PutAction putActionToMerge : putActionList) {
      putsToMerge.add(putActionToMerge.getPut());
      VersionCheckAction checkActionToMerge = putActionToMerge.getVersionCheckAction();
      if (checkActionToMerge != null) {
        checkAction = checkActionToMerge;
      }
    }
    Put put = mergePuts(keyBytes, putsToMerge);
    return new PutAction(put, checkAction);
  }

  /**
   * For each value in put2, add it to put1.
   * 
   * @param put1
   *          The put to modify
   * @param put2
   *          The put to add to put1
   */
  public static void addToPut(Put put1, Put put2) {
    Map<byte[], List<KeyValue>> familyMap = put2.getFamilyMap();
    for (List<KeyValue> keyValueList : familyMap.values()) {
      for (KeyValue keyValue : keyValueList) {
        try {
          put1.add(keyValue);
        } catch (IOException e) {
          throw new HBaseClientException("Could not add KeyValue to put", e);
        }
      }
    }
  }

  /**
   * Add a Collection of Columns to an Operation, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the operation
   * @param operation
   *          The HBase operation to add the columns to
   */
  private static void addColumnsToOperation(Collection<String> columns, Operation operation) {
    // Keep track of whole family additions
    Set<String> familySet = new HashSet<String>();

    // Iterate through each of the required columns
    for (String column : columns) {

      // Split the column by : (family : column)
      String[] familyAndColumn = column.split(":");

      // Check if this is a family only
      if (familyAndColumn.length == 1) {
        // Add family to whole family additions, and add to scanner
        familySet.add(familyAndColumn[0]);
        operation.addFamily(Bytes.toBytes(familyAndColumn[0]));
      } else {
        // Add this column, as long as it's entire family wasn't added.
        if (!familySet.contains(familyAndColumn[0])) {
          operation.addColumn(Bytes.toBytes(familyAndColumn[0]), Bytes.toBytes(familyAndColumn[1]));
        }
      }
    }
  }

  /**
   * Add a Collection of Columns to a Scanner, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the scan
   * @param scan
   *          The scanner object to add the columns to
   */
  public static void addColumnsToScan(Collection<String> columns, final Scan scan) {
    addColumnsToOperation(columns, new Operation() {
      @Override
      public void addColumn(byte[] family, byte[] column) {
        scan.addColumn(family, column);
      }

      @Override
      public void addFamily(byte[] family) {
        scan.addFamily(family);
      }
    });
  }

  /**
   * Add a Collection of Columns to a Get, Only Add Single Columns
   * If Their Family Isn't Already Being Added.
   * @param columns
   *          Collection of columns to add to the Get
   * @param get
   *          The Get object to add the columns to
   */
  public static void addColumnsToGet(Collection<String> columns, final Get get) {
    addColumnsToOperation(columns, new Operation() {
      @Override
      public void addColumn(byte[] family, byte[] column) {
        get.addColumn(family, column);
      }

      @Override
      public void addFamily(byte[] family) {
        get.addFamily(family);
      }
    });
  }
}
