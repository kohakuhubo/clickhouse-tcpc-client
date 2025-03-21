package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.settings.SettingKey;


import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeUtil {

    public static ZoneId chooseTimeZone(NativeContext.ServerContext serverContext) {
        return (boolean) serverContext.getConfigure().settings().getOrDefault(SettingKey.use_client_time_zone, false)
                ? ZoneId.systemDefault() : serverContext.timeZone();
    }

    public static LocalDateTime convertTimeZone(LocalDateTime localDateTime, ZoneId from, ZoneId to) {
        return localDateTime.atZone(from).withZoneSameInstant(to).toLocalDateTime();
    }

    public static long toEpochMilli(final ZonedDateTime zdt) {
        return zdt.toInstant().toEpochMilli();
    }

    public static long toEpochSecond(final ZonedDateTime zdt) {
        return zdt.toInstant().getEpochSecond();
    }

    public static ZonedDateTime toZonedDateTime(final long seconds, final int nanos, final ZoneId tz) {
        Instant i = Instant.ofEpochSecond(seconds, nanos);
        return ZonedDateTime.ofInstant(i, tz);
    }

    public static ZonedDateTime toZonedDateTime(final Timestamp x, final ZoneId tz) {
        Instant i = Instant.ofEpochSecond(x.getTime() / 1000, x.getNanos());
        return ZonedDateTime.ofInstant(i, tz);
    }

    public static Timestamp toTimestamp(final ZonedDateTime zdt,  final ZoneId tz) {
        ZonedDateTime _zdt = tz == null ? zdt : zdt.withZoneSameLocal(tz);
        return Timestamp.from(_zdt.toInstant());
    }
}
