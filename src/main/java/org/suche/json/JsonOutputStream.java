package org.suche.json;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class JsonOutputStream implements AutoCloseable {
	private static final byte[] DIGITS = new byte[200];
	static {
		var c0 = (byte)'0';
		var c1 = (byte)'0';
		for(var i=0; i<100; i++) {
			DIGITS[i*2]   = c0;
			DIGITS[i*2+1] = c1;
			c0++;
			if(c0>'9') { c0 = '0'; c1++; }
		}
	}

	private static final int    BUFFER_SIZE = 128 * 1024;
	private static final ObjectPool<JsonOutputStream> STREAM_POOL = new ObjectPool<>(64, JsonOutputStream::new);
	static         final byte[] NULL_BYTES  = {'n','u','l','l'};
	private static final byte[] TRUE_BYTES  = {'t','r','u','e'};
	private static final byte[] FALSE_BYTES = {'f','a','l','s','e'};
	private static final byte[] MIN_LONG    = "-9223372036854775808".getBytes();
	private static final long[] POW10_L     = { 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L };
	private static final Object EXHAUSTED   = new Object();
	private static final byte   TYPE_COL           = 2;
	private static final byte   TYPE_RECORD        = 3;
	private static final byte   TYPE_ARRAY         = 4;
	private static final byte   TYPE_LIST          = 5;
	private static final byte   TYPE_COMPACT_MAP   = 6;
	private static final byte   TYPE_OBJ_ARRAY     = 7;
	private static final byte   TYPE_POOLED_MAP    = 8;
	public  static final int    PRINT_NULL         = 1 << Flags.printNull         .ordinal();
	public  static final int    PRINT_FALSE        = 1 << Flags.printFalse        .ordinal();
	public  static final int    PRINT_EMPTY        = 1 << Flags.printEmpty        .ordinal();
	public  static final int    PRINT_1_FRACTIONAL = 1 << Flags.printOneFractional.ordinal();
	public  static final int    PRINT_3_FRACTIONAL = 1 << Flags.printOneFractional.ordinal();

	private InternalEngine engine;

	static final class CTX {
		byte   type;
		Object obj;
		Object meta;
		int    idx;
		int    len;

		void set(final byte pType, final Object pObj, final Object pMeta, final int pLen) {
			this.type = pType;
			this.obj  = pObj;
			this.meta = pMeta;
			this.idx  = 0;
			this.len  = pLen;
		}

		void clear() { this.obj = null; this.meta = null; }
	}

	static final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack  = stack64;
		int depth = -1;
		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }

		void push(final byte type, final Object obj, final Object meta, final int len) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(type, obj, meta, len);
		}

		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clear();
			stack = stack64;
			depth = -1;
		}
	}

	private static final class MapDumper implements BiConsumer<Object, Object> {
		Object[] target;
		int      idx;
		@Override public void accept(final Object k, final Object v) {
			target[idx++] = k;
			target[idx++] = v;
		}
	}

	private final byte[] buffer = new byte[BUFFER_SIZE];
	private boolean                                hasCoreTransformer;
	private final MapDumper mapDumper = new MapDumper();
	private int          pos = 0;
	private OutputStream out;
	private final CtxStack  stack = new CtxStack();
	private TimeFormat   timeFormat;
	private boolean skipNull, skipFalse, skipEmpty;
	private int          fractionalLimit;
	private static final Supplier<Object> ERROR = () -> { throw new IllegalStateException("Cyclic reference"); };
	Supplier<Object> cycleMarker = ERROR;
	Object queuedComplex;

	private final Object[][]     arrayPool       = new Object[32][];
	private int                  arrayPoolSize   = 0;

	Object[] takeArray(final int minCapacity) {
		if (arrayPoolSize > 0) {
			final var arr = arrayPool[--arrayPoolSize];
			if (arr.length >= minCapacity) return arr;
		}
		return new Object[Math.max(16, minCapacity)];
	}

	void returnArray(final Object[] arr) {
		if (arr.length > 2048) return;
		if (arrayPoolSize < arrayPool.length) { Arrays.fill(arr, null); arrayPool[arrayPoolSize++] = arr; }
	}

	public enum Flags { printNull, printFalse, printEmpty, printOneFractional, printThreeFractional }

	private final Object DEFAULTOBJ   = new Object();

	private void init0(final InternalEngine pEngine, final OutputStream pOut, final TimeFormat pTimeFormat) {
		this.engine             = pEngine;
		this.out                = pOut;
		this.pos                = 0;
		this.timeFormat         = pTimeFormat==null ? TimeFormatDefault.EPOCH_MILLIS : pTimeFormat;
		this.stack.depth        = -1;
		this.hasCoreTransformer = pEngine.hasCoreTransformer();
	}

	@Override public void close() throws IOException {
		stack.clear();
		try { flushBuffer(); }
		finally {
			this.out = null;
			STREAM_POOL.release(this);
		}
	}

	void init(final InternalEngine pEngine, final OutputStream pOut, final TimeFormat pTimeFormat, final Flags... flags) {
		init0(pEngine, pOut, pTimeFormat);
		var preSkipNull  = true;
		var preSkipFalse = true;
		var preSkipEmpty = true;
		var frac = 6;
		if(null!=flags)for(final var f:flags) switch(f) {
		case printNull            -> preSkipNull  = false;
		case printEmpty           -> preSkipEmpty = false;
		case printFalse           -> preSkipFalse = false;
		case printOneFractional   -> frac      = 1;
		case printThreeFractional -> frac      = 3;
		default                   -> throw new IllegalArgumentException();
		}
		this.skipNull        = preSkipNull ;
		this.skipFalse       = preSkipFalse;
		this.skipEmpty       = preSkipEmpty;
		this.fractionalLimit = frac;
	}

	private void init(final InternalEngine e, final OutputStream newOut, final TimeFormat pTimeFormat, final int flags) {
		init0(e, newOut, pTimeFormat);
		var frac = 6;
		this.skipNull  = (flags & PRINT_NULL)  == 0;
		this.skipFalse = (flags & PRINT_FALSE) == 0;
		this.skipEmpty = (flags & PRINT_EMPTY) == 0;
		if ((flags & PRINT_1_FRACTIONAL) != 0) frac = 1;
		if ((flags & PRINT_3_FRACTIONAL) != 0) frac = 3;
		this.fractionalLimit = frac;
	}

	private JsonOutputStream() {}

	static JsonOutputStream of(final InternalEngine cfg, final OutputStream out, final TimeFormat timeFormat, final Flags... flags)  {
		final var s = STREAM_POOL.acquire();
		s.init(cfg, out, timeFormat, flags);
		return s;
	}

	static JsonOutputStream of(final InternalEngine cfg, final OutputStream out, final TimeFormat timeFormat, final int flags)  {
		final var s = STREAM_POOL.acquire();
		s.init(cfg, out, timeFormat, flags);
		return s;
	}

	private void push(final byte start, final byte type, final Object obj, final Object meta, final int len) throws IOException {
		if (pos >= buffer.length - 1) flushBuffer();
		buffer[pos++] = start;
		stack.push(type, obj, meta, len);
	}

	private Object transform(final Object val) {
		if (val == null || !hasCoreTransformer) return val;
		final Class<?> c = val.getClass();
		if (c == String.class || c == Integer.class || c == Long.class || c == Boolean.class || c == Double.class) return val;
		final var f = engine.transformer(c);
		return f != null ? f.apply(val) : val;
	}

	public void writeObject( final Object root) throws IOException {
		final var transformed = transform(root);
		try {
			handleValue(transformed);
			while (stack.depth >= 0) {
				final var c = stack.stack[stack.depth];
				final var type = c.type;
				var nextVal = EXHAUSTED;
				switch (type) {
				case TYPE_RECORD      -> {
					final var comps = (KeyValueObject[]) c.meta;
					while (c.idx < c.len) {
						final var comp = comps[c.idx++];
						switch (comp.type()) {
						case 1 -> { write(comp.jsonKeyBytes(commaNeeded())); writeNumber((char)0, comp.intGetter   ().applyAsInt   (c.obj)); }
						case 2 -> { write(comp.jsonKeyBytes(commaNeeded())); writeNumber((char)0, comp.longGetter  ().applyAsLong  (c.obj)); }
						case 3 -> { write(comp.jsonKeyBytes(commaNeeded())); writeDouble((char)0, comp.doubleGetter().applyAsDouble(c.obj)); }
						case 4 -> {
							final var bVal = comp.boolGetter().test(c.obj);
							if (skipFalse && !bVal) continue;
							write(comp.jsonKeyBytes(commaNeeded())); writeBoolean(bVal);
						}
						default -> {
							final var val = transform(comp.objGetter().apply(c.obj));
							if (!isSkipped(val)) {
								write(comp.jsonKeyBytes(commaNeeded()));
								nextVal = val;
							}
						}
						}
						if (nextVal != EXHAUSTED) break;
					}
				}
				case TYPE_COMPACT_MAP,TYPE_POOLED_MAP -> {
					final var array = (Object[]) c.obj;
					final var length = c.len;
					var index = c.idx;
					while (index < length) {
						final var key =           array[index++];
						final var val = transform(array[index++]);
						if (isSkipped(val)) continue;
						writeMapKey(key);
						if (!writePrimitiveInline(val)) {
							nextVal = val;
							c.idx = index;
							break;
						}
					}
				}
				case TYPE_OBJ_ARRAY   -> {
					final var array = (Object[]) c.obj;
					while (c.idx < c.len) {
						final var val = transform(array[c.idx++]);
						if (isSkipped(val)) continue;
						writeCommaIfNeeded();
						if (!writePrimitiveInline(val)) { nextVal = val; break; }
					}
				}
				case TYPE_LIST        -> {
					@SuppressWarnings("unchecked")
					final var list = (java.util.List<Object>) c.obj;
					while (c.idx < c.len) {
						final var val = transform(list.get(c.idx++));
						if (isSkipped(val)) continue;
						writeCommaIfNeeded();
						if (!writePrimitiveInline(val)) { nextVal = val; break; }
					}
				}
				case TYPE_COL         -> {
					final Iterator<?> it = (Iterator<?>) c.meta;
					while (it.hasNext()) {
						c.idx++;
						final Object val = it.next();
						if (!isSkipped(val)) {
							writeCommaIfNeeded();
							nextVal = val;
							break;
						}
					}
				}
				case TYPE_ARRAY       -> {
					while (c.idx < c.len) {
						final var val = transform(Array.get(c.obj, c.idx++));
						if (isSkipped(val)) continue;
						writeCommaIfNeeded();
						if (!writePrimitiveInline(val)) { nextVal = val; break; }
					}
				}
				}
				if (nextVal == EXHAUSTED) {
					write(isContainerType(type) ? (byte)']' : (byte)'}');
					if (type == TYPE_POOLED_MAP) returnArray((Object[]) c.obj);
					c.clear();
					stack.depth--;
				} else handleValue(nextVal);
			}
		} catch (final RuntimeException t) { throw t;
		} catch (final Exception        t) {
			if (t instanceof final IOException e) throw e;
			final var x = new RuntimeException(t);
			x.setStackTrace(t.getStackTrace());
			throw x;
		}
	}

	static boolean isContainerType(final byte type) {
		return type == TYPE_COL || type == TYPE_LIST || type == TYPE_ARRAY || type == TYPE_OBJ_ARRAY;
	}

	interface ObjectWriter<T> { void accept(JsonOutputStream s, T v) throws IOException; }

	public interface TimeFormat  {
		void writeDate(JsonOutputStream s, Date     v) throws IOException;
		void writeTemp(JsonOutputStream s, Temporal v) throws IOException;
	}

	public enum TimeFormatDefault implements TimeFormat {
		ISO_STRING   ((s,v)-> s.writeTimestampasText(v.toInstant())
				,     JsonOutputStream::writeTimestampasText),
		EPOCH_MILLIS ((s,v)-> s.writeNumber((char)0, v.getTime())         , (s,v)-> s.writeNumber((char)0, java.time.Instant.from(v).toEpochMilli  ())),
		EPOCH_SECONDS((s,v)-> s.writeNumber((char)0, v.getTime() / 1000L) , (s,v)-> s.writeNumber((char)0, java.time.Instant.from(v).getEpochSecond())),
		;

		ObjectWriter<Date    > writeDate;
		ObjectWriter<Temporal> writeTemp;

		TimeFormatDefault(final ObjectWriter<Date> pWriteDate, final ObjectWriter<Temporal> pWriteTemp) {
			this.writeDate = pWriteDate;
			this.writeTemp = pWriteTemp;
		}
		@Override public void writeDate(final JsonOutputStream s, final Date     v) throws IOException { writeDate.accept(s, v); }
		@Override public void writeTemp(final JsonOutputStream s, final Temporal v) throws IOException { writeTemp.accept(s, v); }
	}

	void write(final byte[] bytes) throws IOException {
		final var len  = bytes.length;
		final var space = buffer.length - pos;
		if (len <= space) {
			System.arraycopy(bytes, 0, buffer, pos, len);
			pos += len;
		} else {
			System.arraycopy(bytes, 0, buffer, pos, space);
			pos = buffer.length;
			flushBuffer();
			final var remaining = len - space;
			if (remaining >= buffer.length) out.write(bytes, space, remaining);
			else {
				System.arraycopy(bytes, space, buffer, 0, remaining);
				pos = remaining;
			}
		}
	}

	void write(final byte   b) throws IOException {
		if (pos == buffer.length) flushBuffer();
		buffer[pos++] = b;
	}

	private void mayFlush(final int require) throws IOException {
		if (pos > 0 && pos + require > buffer.length) flushBuffer();
	}

	private void flushBuffer       () throws IOException { if (pos > 0) { out.write(buffer, 0, pos); pos = 0; } }

	private void writeBaseAscii    (final String s, final int start, final int end) throws IOException {
		for (var i = start; i < end; i++) write((byte) s.charAt(i));
	}

	private void writeBaseAscii    (final char[] c, final int start, final int end) throws IOException {
		for (var i = start; i < end; i++) write((byte) c[i]       );
	}

	//Used by handleSimple(BigInteger), writeFractionalLimited(Number)
	private void writeBaseAscii(final String s) throws IOException {
		final var l = s.length();
		if (pos + l <= buffer.length) {
			// Fast path: Unrolled copying without bounds checking
			for (var i = 0; i < l; i++) {
				buffer[pos++] = (byte) s.charAt(i);
			}
		} else {
			// Slow path: Bounds check for each character
			for (var i = 0; i < l; i++) write((byte) s.charAt(i));
		}
	}
	void writeBoolean(final boolean b) throws IOException { write(b ? TRUE_BYTES : FALSE_BYTES); }

	void writeEscapedString(final String s) throws IOException {
		final var sLen = s.length();
		mayFlush(sLen+2);
		buffer[pos++] = '"';
		var sOff = 0;
		while (sOff < sLen) {
			final var res = JSONString.encodeChunk(s, sOff, sLen, buffer, pos, buffer.length);
			sOff += (int) (res >> 32);
			pos  += (int) res;
			if (sOff < sLen) flushBuffer();
		}
		if (pos == buffer.length) flushBuffer();
		buffer[pos++] = '"';
	}

	// Used by writeSimple(Number.toString)
	private void writeFractionalLimited(final String s) throws IOException {
		if (fractionalLimit < 0) { writeBaseAscii(s); return; }
		final var dot = s.indexOf('.');
		if (dot == -1 || s.indexOf('e', dot) != -1 || s.indexOf('E', dot) != -1) { writeBaseAscii(s); return; }
		final var sLen = s.length();
		final var targetLimit = dot + 1 + fractionalLimit;
		if (targetLimit >= sLen) { writeBaseAscii(s, 0, getCleanLen(s, sLen, dot)); return; }
		final var chars = s.toCharArray();
		var carry = chars[targetLimit] >= '5';
		if (carry) {
			final var lastIdx = targetLimit - 1;
			for (var i = lastIdx; i >= 0; i--) {
				if (chars[i] != '.') {
					if (chars[i] < '9') { chars[i]++; carry = false; break; }
					chars[i] = '0';
				}
			}
			if (carry) {
				write((byte) '1');
				for (var i = 0; i < dot; i++) write((byte) '0');
				return;
			}
		}
		writeBaseAscii(chars, 0, getCleanLen(chars, targetLimit, dot));
	}

	private static int getCleanLen(final String s, int len, final int dotIdx) {
		while (len > dotIdx && (s.charAt(len - 1) == '0' || s.charAt(len - 1) == '.')) if (s.charAt(--len) == '.') break;
		return len;
	}

	private static int getCleanLen(final char[] c, int len, final int dotIdx) {
		while (len > dotIdx && (c[len - 1] == '0' || c[len - 1] == '.')) if (c[--len] == '.') break;
		return len;
	}

	private static final byte[] ZEROS = {'0','0','0','0','0','0','0','0','0'};

	private static final int[] POW10 = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

	private static int getDigits(final int v) {
		// 1. Bit-Länge ermitteln (Hardware LZCNT - 1 Taktzyklus)
		final var bitLength = 32 - Integer.numberOfLeadingZeros(v);

		// 2. Annäherung der Basis-10 Länge berechnen.
		// Multiplikation mit log10(2) angenähert durch (x * 1233) >>> 12
		// Das ergibt fast immer die exakte Anzahl der Ziffern, ist aber extrem schnell.
		var guess = (bitLength * 1233) >>> 12;

		// 3. Korrektur-Check (Ein einziges, extrem vorhersehbares IF)
		// Da guess entweder exakt stimmt oder genau 1 zu niedrig ist,
		// prüfen wir das gegen unsere POW10 Tabelle.
		if (v >= POW10[guess])  guess++;
		return guess;
	}

	private static final byte[] MIN_INT = "-2147483648".getBytes();

	void writeNumber(final char prefix, int val) throws IOException {
		if (pos + 14 > buffer.length) flushBuffer();
		if(0 != prefix) buffer[pos++] = (byte)prefix;
		if (val == 0) {
			buffer[pos++] = '0';
			return;
		}
		if (val == Integer.MIN_VALUE) {
			final var len = MIN_INT.length;
			System.arraycopy(MIN_INT, 0, buffer, pos, len);
			pos += len;
			return;
		}
		if (val < 0) { buffer[pos++] = (byte) '-'; val = -val; }
		final var start = pos + 9;
		var i = start;
		while (val >= 100) {
			final var q = val % 100;
			val /= 100;
			buffer[--i] = DIGITS[q * 2 + 1];
			buffer[--i] = DIGITS[q * 2];
		}
		if (val < 10)  buffer[--i] = (byte) ('0' + val);
		else {
			buffer[--i] = DIGITS[val * 2 + 1];
			buffer[--i] = DIGITS[val * 2];
		}
		final var len = start - i;
		System.arraycopy(buffer, i, buffer, pos, len);
		pos += len;
	}

	void writeNumber(final char prefix, long val) throws IOException {
		if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) { writeNumber(prefix, (int) val); return; }
		if (pos + 22 > buffer.length) flushBuffer();
		if(0 != prefix) buffer[pos++] = (byte)prefix;

		if (val == Long.MIN_VALUE) {
			final var len = MIN_LONG.length;
			System.arraycopy(MIN_LONG, 0, buffer, pos, len);
			pos += len;
			return;
		}
		if (val < 0) { buffer[pos++] = (byte) '-'; val = -val; }
		final var start = pos + 19;
		var i = start;
		while (val >= 100) {
			final var q = (int) (val % 100);
			val /= 100;
			buffer[--i] = DIGITS[q * 2 + 1];
			buffer[--i] = DIGITS[q * 2];
		}
		if (val < 10)  buffer[--i] = (byte) ('0' + val);
		else {
			final var q = (int) val;
			buffer[--i] = DIGITS[q * 2 + 1];
			buffer[--i] = DIGITS[q * 2];
		}
		final var len = start - i;
		System.arraycopy(buffer, i, buffer, pos, len);
		pos += len;
	}

	private void writeDouble(final char prefix, double d) throws IOException {
		if (pos + 28 > buffer.length) flushBuffer();
		if(0 != prefix) buffer[pos++] = (byte)prefix;
		if (Double.isNaN(d) || Double.isInfinite(d)) {
			final var len = NULL_BYTES.length;
			System.arraycopy(NULL_BYTES, 0, buffer, pos, len);
			pos += len;
			return;
		}
		final var l = (long) d;
		if (d == l) { writeNumber((char)0, l); return; }
		var limit = fractionalLimit >= 0 ? fractionalLimit : 6;
		if (limit >= POW10_L.length) limit = POW10_L.length - 1;
		if (d >= 1e14 || d <= -1e14 || (Math.abs(d) < 1e-4)) { writeFractionalLimited(Double.toString(d)); return; }
		if (d < 0) {
			buffer[pos++] = '-';
			d = -d;
		}
		final var multiplier = POW10_L[limit];
		final var total      = (long) (d * multiplier + 0.5d);
		final var intPart    = total / multiplier;
		var fracPart = total % multiplier;
		writeNumber((char)0, intPart);
		if (fracPart == 0) return;
		buffer[pos++] = '.';
		// 1. Führende Nullen berechnen und schreiben
		final var zeros = limit - getDigits((int)fracPart);
		if (zeros > 0) {
			System.arraycopy(ZEROS, 0, buffer, pos, zeros);
			pos += zeros;
		}
		// 2. Trailing Zeros entfernen (wird nur ausgeführt, wenn fracPart > 0 ist, was wir oben gesichert haben)
		while (fracPart % 10 == 0) fracPart /= 10;
		// 3. Den bereinigten Rest schreiben
		writeNumber((char)0, fracPart);
	}

	void writeTimestampasText(final Temporal t) throws IOException {
		mayFlush(26);	// "2000-01-01T00:00:00.001Z"
		if (t == null) {
			final var len = NULL_BYTES.length;
			System.arraycopy(NULL_BYTES, 0, buffer, pos, len);
			pos += len;
			return;
		}
		final var inst = switch (t) {
		case final Instant        v -> v;
		case final LocalDate      v -> v.atStartOfDay(JsonDateTime.BERLIN).toInstant();
		case final LocalDateTime  v -> v.atZone(JsonDateTime.BERLIN).toInstant();
		case final ZonedDateTime  v -> v.toInstant();
		case final OffsetDateTime v -> v.toInstant();
		default                     -> throw new IllegalStateException(t.getClass().getName());
		};
		final var utc = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
		buffer[pos++] = (byte)'"';
		final var year = utc.getYear();
		final var century = year / 100;
		final var decade  = year % 100;
		buffer[pos++] = DIGITS[century * 2];
		buffer[pos++] = DIGITS[century * 2 + 1];
		buffer[pos++] = DIGITS[decade * 2];
		buffer[pos++] = DIGITS[decade * 2 + 1];
		buffer[pos++] = ((byte)'-');
		final var q = utc.getMonthValue();
		buffer[pos++] = DIGITS[q * 2];
		buffer[pos++] = DIGITS[q * 2 + 1];
		buffer[pos++] = ((byte)'-');
		write2(utc.getDayOfMonth());
		buffer[pos++] = ((byte)'T');
		write2(utc.getHour());
		buffer[pos++] = ((byte)':');
		write2(utc.getMinute());
		buffer[pos++] = ((byte)':');
		write2(utc.getSecond());
		buffer[pos++] = ((byte)'.');
		write3( utc.getNano() / 1_000_000);
		buffer[pos++] = ((byte)'Z');
		buffer[pos++] = ((byte)'"');
	}

	private void write3(final int val) {
		buffer[pos++] = (byte)('0' + (val / 100));
		final var q = val % 100;
		buffer[pos++] = DIGITS[q * 2];
		buffer[pos++] = DIGITS[q * 2 + 1];
	}

	private void write2(final int q) {
		buffer[pos++] = DIGITS[q * 2];
		buffer[pos++] = DIGITS[q * 2 + 1];
	}

	boolean commaNeeded() { return(pos == 0 || (buffer[pos - 1] != '[' && buffer[pos - 1] != '{')); }

	void writeCommaIfNeeded() throws IOException { if (pos == 0 || (buffer[pos - 1] != '[' && buffer[pos - 1] != '{')) write((byte)','); }

	boolean isSkipped(final Object v) {
		switch(v) {
		case null                    -> { return skipNull;                   }
		case final String          t -> { return skipEmpty && t.isEmpty();   }
		case final Integer         _ -> { return false;                      }
		case final Long            _ -> { return false;                      }
		case final Double          _ -> { return false;                      }
		case final Boolean         t -> { return skipFalse && !t;            }
		case final CompactList< ?> t -> { return skipEmpty && t.isEmpty(); }
		case final CompactMap<?,?> t -> { return skipEmpty && t.size() == 0; }
		case final Object[]        t -> { return skipEmpty && t.length == 0; }
		case final Collection<?>   t -> { return skipEmpty && t.isEmpty();   }
		case final Map<?, ?>       t -> { return skipEmpty && t.isEmpty();   }
		case final Record          t -> { return skipEmpty && engine.ofComplex(t.getClass()).length == 0; }
		default -> {
			final Class<?> c = v.getClass(); // Liest den Klass-Pointer in O(1)
			if (c.isArray())              return skipEmpty && Array.getLength(v) == 0;
			return false;
		}
		}
	}

	private boolean writePrimitiveInline(final Object val) throws IOException {
		switch(val) {
		case null            -> write(NULL_BYTES);
		case final String  t -> writeEscapedString (t);
		case final Integer t -> writeNumber       ((char)0, t);
		case final Long    t -> writeNumber       ((char)0, t);
		case final Boolean t -> writeBoolean      (t);
		case final Double  t -> writeDouble       ((char)0, t);
		default              -> { return false; }
		}
		return true;
	}

	private void safeEmptyArray() {
		buffer[pos++] = '[';
		buffer[pos++] = ']';
	}

	private void handleMap(final Map<?,?> t) throws IOException {
		final var size = t.size();
		if (size == 0) { safeEmptyArray(); return; } // Optionaler Fast-Path
		final var flat = takeArray(size * 2);
		mapDumper.target = flat;
		mapDumper.idx    = 0;
		t.forEach(mapDumper);
		push((byte)'{', TYPE_POOLED_MAP, flat, null, size * 2);
	}

	private void safeNull() {
		System.arraycopy(NULL_BYTES, 0, buffer, pos, 4);
		pos += 4;
	}

	private boolean handleCycle(final Object obj) throws IOException {
		for (var i = 0; i <= stack.depth; i++) {
			if (stack.stack[i].obj == obj) {
				final var replacement = cycleMarker.get();
				if (replacement == null) safeNull();
				else if (replacement instanceof final String s) writeEscapedString(s);
				else writeObject(replacement);
				return true;
			}
		}
		return false;
	}

	private boolean handleSimple(final Object val) throws IOException {
		if (pos + 32 > buffer.length) flushBuffer();
		if (handleCycle(val)) return true;
		switch(val) {
		case null                      -> safeNull();
		case final String            t -> writeEscapedString(t);
		case final Long              t -> writeNumber((char)0, t);					// Ensure 22
		case final Double            t -> writeDouble((char)0, t);					// Ensure 28
		case final CompactList<?>    t -> push((byte)'[', TYPE_OBJ_ARRAY  , t.getRawData(), null, t.size());
		case final CompactMap<?,?>   t -> push((byte)'{', TYPE_COMPACT_MAP, t.getRawData(), null, t.size() * 2);
		case final Object[]          t -> push((byte)'[', TYPE_OBJ_ARRAY  , t, null, t.length);
		// Record could have an interface for Map/Collection but direct access is faster
		case final Record            t -> { final var parts = engine.ofComplex(t.getClass()); push((byte)'{', TYPE_RECORD, t, parts, parts.length); }
		case final List<?>           t when t instanceof java.util.RandomAccess -> push((byte)'[', TYPE_LIST, t, null, t.size());
		case final Collection<?>     t -> push((byte)'[', TYPE_COL   , t, t.iterator(),  t.size());
		case final Map<?,?>          t -> handleMap(t);
		case final Integer           t -> writeNumber((char)0, t);					// Ensure 14
		case final Boolean           t -> write(t ? TRUE_BYTES : FALSE_BYTES);
		case final Short             t -> writeNumber((char)0, t.longValue());		// Ensure 22
		case final Float             t -> writeDouble((char)0, t.doubleValue());	// Ensure 28
		case final BigInteger        t -> writeBaseAscii(t.toString());
		case final Number            t -> writeFractionalLimited(t.toString());
		case final Enum<?>           t -> writeEscapedString(t.name());
		case final Date              t -> timeFormat.writeDate(this, t);			//  Ensure 26
		case final Temporal          t -> timeFormat.writeTemp(this, t);			//  Ensure 26
		case final double []         t -> { if(t.length>0) { writeDouble('[', t[0]); for(var i=1; i<t.length; i++) { writeDouble(',',t[i]); } write((byte)']'); } else safeEmptyArray(); }
		case final long   []         t -> { if(t.length>0) { writeNumber('[', t[0]); for(var i=1; i<t.length; i++) { writeNumber(',',t[i]); } write((byte)']'); } else safeEmptyArray(); }
		case final int    []         t -> { if(t.length>0) { writeNumber('[', t[0]); for(var i=1; i<t.length; i++) { writeNumber(',',t[i]); } write((byte)']'); } else safeEmptyArray(); }
		case final short  []         t -> { if(t.length>0) { writeNumber('[', t[0]); for(var i=1; i<t.length; i++) { writeNumber(',',t[i]); } write((byte)']'); } else safeEmptyArray(); }
		case final float  []         t -> { if(t.length>0) { writeDouble('[', t[0]); for(var i=1; i<t.length; i++) { writeDouble(',',t[i]); } write((byte)']'); } else safeEmptyArray(); }
		case final boolean[]         t -> { write((byte)'['); if(t.length>0) { write(t[0] ? TRUE_BYTES : FALSE_BYTES); for(var i=1; i<t.length; i++) { write((byte)','); write(t[i] ? TRUE_BYTES : FALSE_BYTES); } } write((byte)']'); }
		default                        -> { if(val.getClass().isArray()) { push((byte)'[', TYPE_ARRAY, val, null, Array.getLength(val)); return true; } return false; }
		}
		return true;
	}

	private void handleValue(final Object val) throws IOException {
		if(null == val) { write(NULL_BYTES); return; }
		if(handleSimple(val)) return;
		final var h = (engine.ofComplex(val.getClass()) instanceof final KeyValueObject[] kv ? kv : DEFAULTOBJ);
		if (h instanceof final KeyValueObject[] parts) push((byte)'{', TYPE_RECORD, val, parts, parts.length);
		else  									       writeEscapedString(val.toString());
	}

	private final Object[] keyCacheKeys = new Object[512];
	private final byte[][] keyCacheVals = new byte[512][];

	void writeEscapedStringKey(final String s, final boolean second) throws IOException {
		final var sLen = s.length();
		mayFlush(sLen+4);
		if(second) buffer[pos++] = ',';
		buffer[pos++] = '"';
		var sOff = 0;
		while (sOff < sLen) {
			final var res = JSONString.encodeChunk(s, sOff, sLen, buffer, pos, buffer.length);
			sOff += (int) (res >> 32);
			pos  += (int) res;
			if (sOff < sLen) flushBuffer();
		}
		if (pos == buffer.length) flushBuffer();
		buffer[pos++] = '"';
		buffer[pos++] = ':';
	}

	private void writeMapKey(final Object key) throws IOException {
		// 1. Fast-Path für Strings
		String outKey;
		final var commaNeeded = commaNeeded();
		if (key instanceof final String str) {
			final var sLen = str.length();
			if (sLen < 32) {
				// Garantieren, dass alles in den Puffer passt:
				// Komma (1) + Quotes (2) + Worst-Case Escapes (sLen * 6) + Doppelpunkt (1)
				mayFlush(sLen * 6 + 4);
				final var hash = System.identityHashCode(key);
				final var idx  = hash & 511;
				if (commaNeeded) buffer[pos++] = ',';
				if (keyCacheKeys[idx] == key) {
					final var cached = keyCacheVals[idx];
					// Schneller Copy-Vorgang direkt in den Puffer, ohne Bounds-Check
					System.arraycopy(cached, 0, buffer, pos, cached.length);
					pos += cached.length;
					return;
				}

				// --- Cache Miss: Direktes Encoding in den Puffer ---
				final var startPos = pos; // Startpunkt für den Cache (NACH dem Komma!)
				buffer[pos++] = '"';

				// encodeChunk schreibt direkt in this.buffer.
				// Gibt die geschriebenen Bytes im unteren 32-Bit-Teil (dstOff) zurück.
				final var res = JSONString.encodeChunk(str, 0, sLen, buffer, pos, buffer.length);
				pos += (int) res;

				buffer[pos++] = '"';
				buffer[pos++] = ':';

				// Exakt passendes Array für den Cache erzeugen und kopieren
				final var encodedLen = pos - startPos;
				final var encoded = new byte[encodedLen];
				System.arraycopy(buffer, startPos, encoded, 0, encodedLen);

				keyCacheKeys[idx] = key;
				keyCacheVals[idx] = encoded;
				return;
			}
			outKey = str;
			// String >= 32 Zeichen (Fallback)
		} else outKey = String.valueOf(key);

		// Non-String Keys (Fallback)
		writeEscapedStringKey(outKey, commaNeeded);
	}
}