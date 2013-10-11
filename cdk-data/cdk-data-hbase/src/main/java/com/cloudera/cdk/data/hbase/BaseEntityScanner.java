// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;

/**
 * Base EntityScanner implementation. This EntityScanner will use an
 * EntityMapper while scanning rows in HBase, and will map each row to KeyEntity
 * pairs.
 * 
 * @param <K>
 *          The underlying key record type.
 * @param <E>
 *          The entity type this scanner scans.
 */
public class BaseEntityScanner<K, E> implements EntityScanner<K, E> {

  private final EntityMapper<K, E> entityMapper;
  private final HTablePool tablePool;
  private final String tableName;
  private Scan scan;
  private ResultScanner resultScanner;

  /**
   * @param scan
   *          The Scan object that will be used
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          The HTablePool instance to get a table to open a scanner on.
   * @param tableName
   *          The table name to perform the scan on.
   * @param entityMapper
   *          The EntityMapper to map rows to entities.
   * @param transactional
   *          true if this is a transactional scan.
   */
  public BaseEntityScanner(Scan scan, HTablePool tablePool, String tableName,
      EntityMapper<K, E> entityMapper) {
    this.scan = scan;
    this.entityMapper = entityMapper;
    this.tablePool = tablePool;
    this.tableName = tableName;
  }

  /**
   * Construct a BaseEntityScanner using a Builder
   * 
   * @param scanBuilder
   *          Build object to construct the BaseEntityScanner with
   */
  private BaseEntityScanner(Builder<K, E> scanBuilder) {
    this.entityMapper = scanBuilder.getEntityMapper();
    this.tablePool = scanBuilder.getTablePool();
    this.tableName = scanBuilder.getTableName();
    this.scan = new Scan();

    if (scanBuilder.getStartKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serialize(
          scanBuilder.getStartKey());
      this.scan.setStartRow(keyBytes);
    } else if (scanBuilder.getPartialStartKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serializePartial(
          scanBuilder.getPartialStartKey());
      this.scan.setStartRow(keyBytes);
    }

    if (scanBuilder.getStopKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serialize(
          scanBuilder.getStopKey());
      this.scan.setStopRow(keyBytes);
    } else if (scanBuilder.getPartialStopKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serializePartial(
          scanBuilder.getPartialStopKey());
      this.scan.setStopRow(keyBytes);
    }

    if (scanBuilder.getCaching() != 0) {
      this.scan.setCaching(scanBuilder.getCaching());
    }

    if (scanBuilder.getEntityMapper() != null) {
      HBaseUtils.addColumnsToScan(entityMapper.getRequiredColumns(), this.scan);
    }

    // If Filter List Was Built, Add It To The Scanner
    if (scanBuilder.getFilterList().size() > 0) {
      // Check if this is a PASS_ALL or PASS_ONE List
      if (scanBuilder.getPassAllFilters()) {
        FilterList filterList = new FilterList(
            FilterList.Operator.MUST_PASS_ALL, scanBuilder.getFilterList());
        this.scan.setFilter(filterList);
      } else {
        FilterList filterList = new FilterList(
            FilterList.Operator.MUST_PASS_ONE, scanBuilder.getFilterList());
        this.scan.setFilter(filterList);
      }
    }

    for (ScanModifier scanModifier : scanBuilder.getScanModifiers()) {
      this.scan = scanModifier.modifyScan(this.scan);
    }
  }

  @Override
  public Iterator<KeyEntity<K, E>> iterator() {
    final Iterator<Result> iterator = resultScanner.iterator();
    return new Iterator<KeyEntity<K, E>>() {

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public KeyEntity<K, E> next() {
        Result result = iterator.next();
        return entityMapper.mapToEntity(result);
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }

  @Override
  public void open() {
    HTableInterface table = null;
    try {
      table = tablePool.getTable(tableName);
      try {
        resultScanner = table.getScanner(scan);
      } catch (IOException e) {
        throw new HBaseClientException("Failed to fetch scanner", e);
      }
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          throw new HBaseClientException("Error putting table back into pool",
              e);
        }
      }
    }
  }

  @Override
  public void close() {
    resultScanner.close();
  }

  /**
   * Scanner builder for BaseEntityScanner
   * 
   * @param <K>
   * @param <E>
   */
  public static class Builder<K, E> extends EntityScannerBuilder<K, E> {

    public Builder(HTablePool tablePool, String tableName,
        EntityMapper<K, E> entityMapper) {
      super(tablePool, tableName, entityMapper);
    }

    @Override
    public BaseEntityScanner<K, E> build() {
      BaseEntityScanner<K, E> scanner = new BaseEntityScanner<K, E>(this);
      scanner.open();
      return scanner;
    }

  }
}
