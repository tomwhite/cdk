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
package com.cloudera.cdk.data.partition;

import com.google.common.annotations.Beta;
import java.text.NumberFormat;
import java.util.Calendar;
import javax.annotation.Nonnull;

@Beta
public class MonthFieldPartitioner extends CalendarFieldPartitioner {
  private final NumberFormat format;

  public MonthFieldPartitioner(String sourceName, String name) {
    super(sourceName, name, Calendar.MONTH, 12);
    format = NumberFormat.getIntegerInstance();
    format.setMinimumIntegerDigits(2);
    format.setMaximumIntegerDigits(2);
  }

  @Override
  public Object apply(@Nonnull Object value) {
    Long timestamp = (Long) value;
    cal.setTimeInMillis(timestamp);
    return cal.get(calendarField) + 1; // Calendar month is 0-based
  }

  @Override
  public String valueToString(Object value) {
    return format.format(value);
  }
}
