/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

class TransformUtil {

  private TransformUtil() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final int EPOCH_YEAR = EPOCH.getYear();

  static String humanYear(int yearOrdinal) {
    StringBuilder sb = new StringBuilder(4);
    appendLeadingZeros(sb, EPOCH_YEAR + yearOrdinal, 4);
    return sb.toString();
  }

  static String humanMonth(int monthOrdinal) {
    StringBuilder sb = new StringBuilder(7);
    appendLeadingZeros(sb, EPOCH_YEAR + Math.floorDiv(monthOrdinal, 12), 4);
    sb.append('-');
    appendLeadingZeros(sb, 1 + Math.floorMod(monthOrdinal, 12), 2);
    return sb.toString();
  }

  static String humanDay(int dayOrdinal) {
    OffsetDateTime day = EPOCH.plusDays(dayOrdinal);
    StringBuilder sb = new StringBuilder(10);
    appendLeadingZeros(sb, day.getYear(), 4);
    sb.append('-');
    appendLeadingZeros(sb, day.getMonth().getValue(), 2);
    sb.append('-');
    appendLeadingZeros(sb, day.getDayOfMonth(), 2);
    return sb.toString();
  }

  static String humanTime(Long microsFromMidnight) {
    return LocalTime.ofNanoOfDay(microsFromMidnight * 1000).toString();
  }

  static String humanTimestampWithZone(Long timestampMicros) {
    return ChronoUnit.MICROS.addTo(EPOCH, timestampMicros).toString();
  }

  static String humanTimestampWithoutZone(Long timestampMicros) {
    return ChronoUnit.MICROS.addTo(EPOCH, timestampMicros).toLocalDateTime().toString();
  }

  static String humanHour(int hourOrdinal) {
    OffsetDateTime time = EPOCH.plusHours(hourOrdinal);
    StringBuilder sb = new StringBuilder(13);
    appendLeadingZeros(sb, time.getYear(), 4);
    sb.append('-');
    appendLeadingZeros(sb, time.getMonth().getValue(), 2);
    sb.append('-');
    appendLeadingZeros(sb, time.getDayOfMonth(), 2);
    sb.append('-');
    appendLeadingZeros(sb, time.getHour(), 2);
    return sb.toString();
  }

  static String base64encode(ByteBuffer buffer) {
    // use direct encoding because all of the encoded bytes are in ASCII
    return StandardCharsets.ISO_8859_1.decode(Base64.getEncoder().encode(buffer)).toString();
  }

  private static void appendLeadingZeros(StringBuilder sb, int value, int digits) {
    String strValue = Integer.toString(value);
    int padding = digits - strValue.length();

    for (int i = 0; i < padding; i++) {
      sb.append('0');
    }
    sb.append(strValue);
  }
}
