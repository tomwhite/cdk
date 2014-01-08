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

import com.cloudera.cdk.data.Marker;
import javax.annotation.Nullable;

public class RangePredicates {

  public static RangePredicate undefined() {
    return new RangePredicate() {
      @Override
      public boolean apply(@Nullable Marker input) {
        return false;
      }
      @Override
      public MarkerRange getRange() {
        return MarkerRange.UNDEFINED;
      }
    };
  }

  public static RangePredicate all(final MarkerComparator markerComparator) {
    return new RangePredicate() {
      @Override
      public boolean apply(@Nullable Marker input) {
        return true;
      }
      @Override
      public MarkerRange getRange() {
        return new MarkerRange(markerComparator);
      }
    };
  }

  private static class MarkerRangePredicate implements RangePredicate {

    private final MarkerRange range;

    public MarkerRangePredicate(MarkerRange range) {
      this.range = range;
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return range.contains(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }
  }

  private static class AndRangePredicate implements RangePredicate {

    private final RangePredicate p1;
    private final RangePredicate p2;
    private final MarkerRange range;

    public AndRangePredicate(RangePredicate p1, RangePredicate p2) {
      this.p1 = p1;
      this.p2 = p2;
      this.range = p1.getRange().combine(p2.getRange());
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return p1.apply(input) && p2.apply(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }
  }

  public static RangePredicate from(MarkerComparator comparator, Marker start) {
    return new MarkerRangePredicate(new MarkerRange(comparator).from(start));
  }

  public static RangePredicate fromAfter(MarkerComparator comparator, Marker start) {
    return new MarkerRangePredicate(new MarkerRange(comparator).fromAfter(start));
  }

  public static RangePredicate to(MarkerComparator comparator, Marker end) {
    return new MarkerRangePredicate(new MarkerRange(comparator).to(end));
  }

  public static RangePredicate toBefore(MarkerComparator comparator, Marker end) {
    return new MarkerRangePredicate(new MarkerRange(comparator).toBefore(end));
  }

  public static RangePredicate of(MarkerComparator comparator, Marker partial) {
    return new MarkerRangePredicate(new MarkerRange(comparator).of(partial));
  }

  public static RangePredicate and(RangePredicate p1, RangePredicate p2) {
    return new AndRangePredicate(p1, p2);
  }

}
