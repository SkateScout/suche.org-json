package org.suche.json;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.time.zone.ZoneRules;

public final class FastDateTime {
	private static final ZoneId    BERLIN_ZONE  = ZoneId.of("Europe/Berlin");
	private static final ZoneRules BERLIN_RULES = BERLIN_ZONE.getRules();
	// Field names for parseInt
	private static final String F_YEAR        = "year";
	private static final String F_MONTH       = "month";
	private static final String F_DAY         = "day";
	private static final String F_HOUR        = "hour";
	private static final String F_MINUTE      = "minute";
	private static final String F_SECOND      = "second";
	private static final String F_FRACTIONALS = "fractionals";

	// Error messages
	private static final String ERR_TOO_SHORT      = "String too short for OffsetDateTime";
	private static final String ERR_TOO_LONG       = "String too long for OffsetDateTime";
	private static final String ERR_TOO_SHORT_TIME = "String too short for time";
	private static final String ERR_TOO_LONG_TIME  = "String too long for time";
	private static final String ERR_SEP_YEAR       = "Missing '-' after year";
	private static final String ERR_SEP_MONTH      = "Missing '-' after month";
	private static final String ERR_SEP_DATE       = "Missing 'T' after date";
	private static final String ERR_SEP_HOUR       = "Missing ':' after hour";
	private static final String ERR_SEP_MINUTE     = "Missing ':' after minute";
	private static final String ERR_FRAC_MISSING   = "Missing fractional after dot";
	private static final String ERR_FRAC_TOO_LONG  = "More than 9 fractionals";
	private static final String ERR_GARBAGE        = "Garbage after dateTime";
	private static final String ERR_DATE_PART      = "Invalid date part";
	private static final String ERR_TIME_PART      = "Invalid time part";
	private static final String ERR_ZONE_OFFSET    = "Invalid zone / offset";

	private static int parseInt(final String field, final String s, final int off, final int end, final int min , final int max) {
		var v = 0;
		char c;
		for(var i=off; i<end;i++) {
			if ( (c = (char)(s.charAt( i) - '0')) > 9) throw invalid("Invalid "+field+" is not numeric", s,  i);
			v = v * 10 + c;
		}
		if(v < min) throw invalid("Invalid "+field+" '"+v+"' is lower than "+min, s,  end);
		if(v > max) throw invalid("Invalid "+field+" '"+v+"' is greater than "+max, s,  end);
		return v;
	}

	private static int parseInt(final String field, final byte[] s, final int off, final int end, final int min , final int max) {
		var v = 0;
		char c;
		for(var i=off; i<end;i++) {
			if ( (c = (char)(s[i] - '0')) > 9) throw invalid("Invalid "+field+" is not numeric", s,  i);
			v = v * 10 + c;
		}
		if(v < min) throw invalid("Invalid "+field+" '"+v+"' is lower than "+min, s,  end);
		if(v > max) throw invalid("Invalid "+field+" '"+v+"' is greater than "+max, s,  end);
		return v;
	}

	private static DateTimeParseException invalid(final String mesg, final Object in, final int idx) {
		if(in instanceof final String val) return new DateTimeParseException(mesg, val, idx);
		if(in instanceof final byte[] val) {
			final var safeIdx = Math.clamp(idx, 0, val.length - 1);
			return new DateTimeParseException(mesg, Character.toString(val[safeIdx]), idx);
		}
		return new DateTimeParseException(mesg, "", idx);
	}

	private static <T extends Temporal>T  build(final Class<T> tc, final int off, final Object in, final boolean utc, final int year, final int month, final int dayOfMonth, final int hour, final int minute, final int second, final int nanoOfSecond) {
		var t = 0;
		if(tc == LocalTime.class) return tc.cast(LocalTime.of(hour, minute, second, nanoOfSecond));
		try {
			final var date = LocalDate.of(year, month, dayOfMonth);
			if(tc == LocalDate.class) return tc.cast(date);
			t++;
			final var time = LocalTime.of(hour, minute, second, nanoOfSecond);
			t++;
			final var dt = LocalDateTime.of(date, time);
			if(tc == LocalDateTime .class) return tc.cast(dt);
			if(tc == OffsetDateTime.class) return tc.cast(OffsetDateTime.of(dt, utc ? ZoneOffset.UTC : BERLIN_RULES.getOffset(dt)));
			if(tc == ZonedDateTime .class) return tc.cast(ZonedDateTime.of(dt, utc ? ZoneId.of("UTC"): BERLIN_ZONE));
			if(tc == Instant       .class) return tc.cast(dt.toInstant(utc ? ZoneOffset.UTC : BERLIN_RULES.getOffset(dt)));
			throw new IllegalArgumentException("Class: "+tc.getCanonicalName());
		} catch (final Throwable _) {
			switch(t) {
			case 0  -> throw invalid(ERR_DATE_PART  , in, off+ 8);
			case 1  -> throw invalid(ERR_TIME_PART  , in, off+11);
			default -> throw invalid(ERR_ZONE_OFFSET, in, off   );
			}
		}
	}


	private static LocalTime parseLocalTime(final String s) {
		if (s == null || s.length() == 0) return null;
		final var len = s.length();
		if (len < 8                           ) throw invalid(ERR_TOO_SHORT_TIME , s, 0);
		if (len > 18                          ) throw invalid(ERR_TOO_LONG_TIME , s, 0);
		if (s.charAt(2) != ':'                ) throw invalid(ERR_SEP_HOUR  , s, 2);
		if (s.charAt(5) != ':'                ) throw invalid(ERR_SEP_MINUTE, s, 5);
		final var hour        = parseInt(F_HOUR  , s, 0   , 0+2,0, 23);
		final var minute      = parseInt(F_MINUTE, s, 0+ 3, 0+5,0, 59);
		final var second      = parseInt(F_SECOND, s, 0+ 6, 0+8,0, 59);
		var nanoOfSecond = 0;	// 3. Optionale Millisekunden und Offset behandeln
		var idx = 0+8;
		if (idx < len && s.charAt(idx) == '.') {
			idx++;
			final var fracDigits = len - idx;
			if (fracDigits < 1) throw invalid(ERR_FRAC_MISSING , s, idx);
			if (fracDigits > 9) throw invalid(ERR_FRAC_TOO_LONG, s, idx);
			final var fracVal = parseInt(F_FRACTIONALS, s, idx, len, 0, 999_999_999);
			var scale = 1;
			for (var i = fracDigits; i < 9; i++) scale *= 10;
			nanoOfSecond = fracVal * scale;
			idx = len;
		}
		if (idx != len) throw invalid(ERR_GARBAGE, s, idx);
		return LocalTime.of(hour, minute, second, nanoOfSecond);
	}

	public static <T extends Temporal>T parseFast(final Class<T> tc, final String s) {
		if (s == null) return null;
		var len = s.length();
		if (0 == len) return null;
		if(len < 19 && tc == LocalTime.class ) return tc.cast(parseLocalTime(s));
		char c;
		if (len < 19                             ) throw invalid(ERR_TOO_SHORT , s, len);
		if (len > 30                             ) throw invalid(ERR_TOO_LONG  , s, len);
		if (s.charAt( 4) != '-'                  ) throw invalid(ERR_SEP_YEAR  , s,  4);
		if (s.charAt( 7) != '-'                  ) throw invalid(ERR_SEP_MONTH , s,  7);
		if ((c = s.charAt(10)) != 'T' && c != ' ') throw invalid(ERR_SEP_DATE   ,s, 10);
		if (s.charAt(13) != ':'                  ) throw invalid(ERR_SEP_HOUR  , s, 13);
		if (s.charAt(16) != ':'                  ) throw invalid(ERR_SEP_MINUTE, s, 16);
		final var year        = parseInt(F_YEAR  , s,  0,  4, 1970, 2100);
		final var month       = parseInt(F_MONTH , s,  5,  7,    1,   12);
		final var dayOfMonth  = parseInt(F_DAY   , s,  8, 10,    1,   31);
		final var hour        = parseInt(F_HOUR  , s, 11, 13,    0,   23);
		final var minute      = parseInt(F_MINUTE, s, 14, 16,    0,   59);
		final var second      = parseInt(F_SECOND, s, 17, 19,    0,   59);
		var nanoOfSecond = 0;	// 3. Optionale Millisekunden und Offset behandeln
		var idx = 19;
		var utc = false;
		if(s.charAt(len-1) == 'Z') { len--; utc = true; }
		if (idx < len && s.charAt(idx) == '.') {
			idx++; // Direkt hinter den Punkt springen
			final var fracDigits = len - idx;
			if (fracDigits < 1) throw invalid(ERR_FRAC_MISSING , s, idx);
			if (fracDigits > 9) throw invalid(ERR_FRAC_TOO_LONG, s, idx);
			final var fracVal = parseInt(F_FRACTIONALS, s, idx, len, 0, 999_999_999);
			var scale = 1;
			for (var i = fracDigits; i < 9; i++) scale *= 10;
			nanoOfSecond = fracVal * scale;
			idx = len;
		}
		if (idx != len) throw invalid(ERR_GARBAGE, s, idx);
		return build(tc, 0, s, utc, year, month, dayOfMonth, hour, minute, second, nanoOfSecond);
	}

	private static LocalTime parseLocalTime(final byte[] s, final int off, final int end) {
		if (s == null || end == off) return null;
		if(off < 0 || end < off || s.length < end   ) throw invalid("Invalid buffer", "", s.length);
		final var len = end - off;
		if (len < 8                           ) throw invalid(ERR_TOO_SHORT_TIME , s, off  );
		if (len > 18                          ) throw invalid(ERR_TOO_LONG_TIME  , s, off  );
		if (s[off+2] != ':'                   ) throw invalid(ERR_SEP_HOUR       , s, off+2);
		if (s[off+5] != ':'                   ) throw invalid(ERR_SEP_MINUTE     , s, off+5);
		final var hour        = parseInt(F_HOUR  , s, off   , off+2,0, 23);
		final var minute      = parseInt(F_MINUTE, s, off+ 3, off+5,0, 59);
		final var second      = parseInt(F_SECOND, s, off+ 6, off+8,0, 59);
		var nanoOfSecond = 0;	// 3. Optionale Millisekunden und Offset behandeln
		var idx = off+8;
		if (idx < end && s[idx] == '.') {
			idx++;
			final var fracDigits = end - idx;
			if (fracDigits < 1) throw invalid(ERR_FRAC_MISSING , s, idx);
			if (fracDigits > 9) throw invalid(ERR_FRAC_TOO_LONG, s, idx);
			final var fracVal = parseInt(F_FRACTIONALS, s, idx, end, 0, 999_999_999);
			var scale = 1;
			for (var i = fracDigits; i < 9; i++) scale *= 10;
			nanoOfSecond = fracVal * scale;
			idx = end;
		}
		if (idx != end) throw invalid(ERR_GARBAGE, s, idx);
		return LocalTime.of(hour, minute, second, nanoOfSecond);
	}

	public static <T extends Temporal>T parseFast(final Class<T> tc, final byte[] s, final int off, int end) {
		if (s == null || end == off) return null;
		final var len = end - off;
		if(len < 19 && tc == LocalTime.class) return tc.cast(parseLocalTime(s, off, end));
		char c;
		if(off < 0 || end < off || s.length < end   ) throw invalid("Invalid buffer", "", s.length);
		if (len < 19                          ) throw invalid(ERR_TOO_SHORT , s, off);
		if (s[off+ 4] != '-'                        ) throw invalid(ERR_SEP_YEAR  , s, off+ 4);
		if (s[off+ 7] != '-'                        ) throw invalid(ERR_SEP_MONTH , s, off+ 7);
		if ((c = (char)s[off+10]) != 'T' && c != ' ') throw invalid(ERR_SEP_DATE  , s, off+10);
		if (s[off+13] != ':'                        ) throw invalid(ERR_SEP_HOUR  , s, off+13);
		if (s[off+16] != ':'                        ) throw invalid(ERR_SEP_MINUTE, s, off+16);
		final var year        = parseInt(F_YEAR  , s, off+ 0, off+ 4, 1970, 2100);
		final var month       = parseInt(F_MONTH , s, off+ 5, off+ 7,    1,   12);
		final var dayOfMonth  = parseInt(F_DAY   , s, off+ 8, off+10,    1,   31);
		final var hour        = parseInt(F_HOUR  , s, off+11, off+13,    0,   23);
		final var minute      = parseInt(F_MINUTE, s, off+14, off+16,    0,   59);
		final var second      = parseInt(F_SECOND, s, off+17, off+19,    0,   59);
		var nanoOfSecond = 0;	// 3. Optionale Millisekunden und Offset behandeln
		var idx = off+19;
		var utc = false;
		if(s[end-1] == 'Z') { end--; utc = true; }
		if (idx < end && s[idx] == '.') {
			idx++;
			final var fracDigits = end - idx;
			if (fracDigits < 1) throw invalid(ERR_FRAC_MISSING , s, idx);
			if (fracDigits > 9) throw invalid(ERR_FRAC_TOO_LONG, s, idx);
			final var fracVal = parseInt(F_FRACTIONALS, s, idx, end, 0, 999_999_999);
			var scale = 1;
			for (var i = fracDigits; i < 9; i++) scale *= 10;
			nanoOfSecond = fracVal * scale;
			idx = end;
		}
		if (idx != end) throw invalid(ERR_GARBAGE, s, idx);
		return build(tc,off, s, utc, year, month, dayOfMonth, hour, minute, second, nanoOfSecond);
	}

	public static <T extends Temporal>T  parseFast(final Class<T> tc, final byte[] s) {
		return s == null ? null : parseFast(tc, s, 0, s.length);
	}

	public static void main(final String[] argc) {
		final var t = "2006-01-01T12:01:01.00Z";
		System.out.println(parseFast(ZonedDateTime.class, t));
		System.out.println(ZonedDateTime.parse(t));
	}

}