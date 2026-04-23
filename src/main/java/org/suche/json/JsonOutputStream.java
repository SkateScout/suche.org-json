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
import java.util.Map;
import java.util.function.Supplier;

public final class JsonOutputStream implements AutoCloseable {
	private static final int    BUFFER_SIZE = 128 * 1024;
	private static final ObjectPool<JsonOutputStream> STREAM_POOL = new ObjectPool<>(64, JsonOutputStream::new);
	static         final byte[] NULL_BYTES  = {'n','u','l','l'};
	private static final byte[] TRUE_BYTES  = {'t','r','u','e'};
	private static final byte[] FALSE_BYTES = {'f','a','l','s','e'};
	private static final byte[] MIN_LONG    = "-9223372036854775808".getBytes();
	private static final long[] POW10_L     = { 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L };
	private static final Object EXHAUSTED   = new Object();
	private static final byte   TYPE_MAP           = 1;
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

		void set(final byte type, final Object obj, final Object meta, final int len) {
			this.type = type;
			this.obj  = obj;
			this.meta = meta;
			this.idx  = 0;
			this.len  = len;
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

	private static final class MapDumper implements java.util.function.BiConsumer<Object, Object> {
		Object[] target;
		int      idx;
		@Override public void accept(final Object k, final Object v) { target[idx++] = k; target[idx++] = v; }
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
	public Supplier<Object> cycleMarker = ERROR;

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

	private final Object DEFAULTOBJ   = new Object();

	private void init0(final InternalEngine engine, final OutputStream out, final TimeFormat timeFormat) {
		this.engine             = engine;
		this.out                = out;
		this.pos                = 0;
		this.timeFormat         = timeFormat==null ? TimeFormatDefault.EPOCH_MILLIS : timeFormat;
		this.stack.depth        = -1;
		this.hasCoreTransformer = engine.hasCoreTransformer();
	}

	@Override public void close() throws IOException {
		stack.clear();
		try { flushBuffer(); }
		finally {
			this.out = null;
			STREAM_POOL.release(this);
		}
	}

	void init(final InternalEngine engine, final OutputStream out, final TimeFormat timeFormat, final Flags... flags) {
		init0(engine, out, timeFormat);
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
		}
		this.skipNull        = preSkipNull ;
		this.skipFalse       = preSkipFalse;
		this.skipEmpty       = preSkipEmpty;
		this.fractionalLimit = frac;
	}

	private void init(final InternalEngine e, final OutputStream out, final TimeFormat timeFormat, final int flags) {
		init0(e, out, timeFormat);
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
		final var f = engine.transformer(val.getClass());
		return f != null ? f.apply(val) : val;
	}

	private boolean handleCycle(final Object obj) throws IOException {
		for (var i = 0; i <= stack.depth; i++) {
			if (stack.stack[i].obj == obj) {
				final var replacement = cycleMarker.get();
				if (replacement == null) write(NULL_BYTES);
				else if (replacement instanceof final String s) writeEscapedString(s);
				else writeObject(replacement);
				return true;
			}
		}
		return false;
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
						case 1 -> { writeCommaIfNeeded(); write(comp.jsonKeyBytes()); write((byte)':'); writeNumber(comp.intGetter   ().applyAsInt   (c.obj)); }
						case 2 -> { writeCommaIfNeeded(); write(comp.jsonKeyBytes()); write((byte)':'); writeNumber(comp.longGetter  ().applyAsLong  (c.obj)); }
						case 3 -> { writeCommaIfNeeded(); write(comp.jsonKeyBytes()); write((byte)':'); writeDouble(comp.doubleGetter().applyAsDouble(c.obj)); }
						case 4 -> {
							final var bVal = comp.boolGetter().test(c.obj);
							if (skipFalse && !bVal) continue;
							writeCommaIfNeeded(); write(comp.jsonKeyBytes()); write((byte)':'); writeBoolean(bVal);
						}
						default -> {
							final var val = transform(comp.objGetter().apply(c.obj));
							if (!isSkipped(val)) {
								writeCommaIfNeeded(); write(comp.jsonKeyBytes()); write((byte)':');
								nextVal = val;
							}
						}
						}
						if (nextVal != EXHAUSTED) break;
					}
				}
				case TYPE_COMPACT_MAP,TYPE_POOLED_MAP -> {
					final var array = (Object[]) c.obj;
					while (c.idx < c.len) {
						final var key =           array[c.idx++];
						final var val = transform(array[c.idx++]);
						if (isSkipped(val)) continue;
						writeMapKey(key);
						if (!writePrimitiveInline(val)) { nextVal = val; break; }
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
				case TYPE_MAP         -> {
					final Iterator<?> it = (Iterator<?>) c.meta;
					while (it.hasNext()) {
						c.idx++;
						final Map.Entry<?, ?> entry = (Map.Entry<?, ?>) it.next();
						final var val = transform(entry.getValue());
						if (!isSkipped(val)) {
							writeMapKey(entry.getKey());
							nextVal = val;
							break;
						}
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
		} catch (final Throwable        t) {
			if (t instanceof final IOException e) throw e;
			final var x = new RuntimeException(t);
			x.setStackTrace(t.getStackTrace());
			throw x;
		}
	}

	private boolean isContainerType(final byte type) {
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
		EPOCH_MILLIS ((s,v)-> s.writeNumber(v.getTime())         , (s,v)-> s.writeNumber(java.time.Instant.from(v).toEpochMilli  ())),
		EPOCH_SECONDS((s,v)-> s.writeNumber(v.getTime() / 1000L) , (s,v)-> s.writeNumber(java.time.Instant.from(v).getEpochSecond())),
		;

		ObjectWriter<Date    > writeDate;
		ObjectWriter<Temporal> writeTemp;

		TimeFormatDefault(final ObjectWriter<Date> writeDate, final ObjectWriter<Temporal> writeTemp) {
			this.writeDate = writeDate;
			this.writeTemp = writeTemp;
		}
		@Override public void writeDate(final JsonOutputStream s, final Date     v) throws IOException { writeDate.accept(s, v); }
		@Override public void writeTemp(final JsonOutputStream s, final Temporal v) throws IOException { writeTemp.accept(s, v); }
	}

	Void write(final byte[] bytes) throws IOException {
		final var len = bytes.length;
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
		return null;
	}

	void write(final byte   b) throws IOException {
		if (pos == buffer.length) flushBuffer(); buffer[pos++] = b;
	}

	private void mayFlush(final int require) throws IOException {
		if (pos > 0 && pos + require > buffer.length) flushBuffer();
	}

	private void flushBuffer       () throws IOException { if (pos > 0) { out.write(buffer, 0, pos); pos = 0; } }

	private Void writeBaseAscii    (final String s, final int start, final int end) throws IOException {
		for (var i = start; i < end; i++) write((byte) s.charAt(i));
		return null;
	}

	private Void writeBaseAscii    (final char[] c, final int start, final int end) throws IOException {
		for (var i = start; i < end; i++) write((byte) c[i]       );
		return null;
	}

	private Void writeBaseAscii    (final String s) throws IOException {
		final var l = s.length();
		for (var i = 0; i < l; i++) write((byte) s.charAt(i));
		return null;
	}

	private void writeBoolean(final boolean b) throws IOException { write(b ? TRUE_BYTES : FALSE_BYTES); }

	private void writeEscapedString(final String s) throws IOException {
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

	private Void writeFractionalLimited(final String s) throws IOException {
		if (fractionalLimit < 0) return writeBaseAscii(s);
		final var dot = s.indexOf('.');
		if (dot == -1 || s.indexOf('e', dot) != -1 || s.indexOf('E', dot) != -1) return writeBaseAscii(s);
		final var sLen = s.length();
		final var targetLimit = dot + 1 + fractionalLimit;
		if (targetLimit >= sLen) return writeBaseAscii(s, 0, getCleanLen(s, sLen, dot));
		final var chars = s.toCharArray();
		var carry = chars[targetLimit] >= '5';
		if (carry) {
			final var lastIdx = targetLimit - 1;
			for (var i = lastIdx; i >= 0; i--) {
				if (chars[i] == '.') continue;
				if (chars[i] < '9') { chars[i]++; carry = false; break; }
				chars[i] = '0';
			}
			if (carry) {
				write((byte) '1');
				for (var i = 0; i < dot; i++) write((byte) '0');
				return null;
			}
		}
		return writeBaseAscii(chars, 0, getCleanLen(chars, targetLimit, dot));
	}

	private int getCleanLen(final String s, int len, final int dotIdx) {
		while (len > dotIdx && (s.charAt(len - 1) == '0' || s.charAt(len - 1) == '.')) if (s.charAt(--len) == '.') break;
		return len;
	}

	private int getCleanLen(final char[] c, int len, final int dotIdx) {
		while (len > dotIdx && (c[len - 1] == '0' || c[len - 1] == '.')) if (c[--len] == '.') break;
		return len;
	}

	private Void writeDouble(double d) throws IOException {
		if (Double.isNaN(d) || Double.isInfinite(d)) return write(NULL_BYTES);
		final var l = (long) d;
		if (d == l) { writeNumber(l); return null; }
		var limit = fractionalLimit >= 0 ? fractionalLimit : 6;
		if (limit >= POW10_L.length) limit = POW10_L.length - 1;
		if (d >= 1e14 || d <= -1e14 || (Math.abs(d) < 1e-4)) return writeFractionalLimited(Double.toString(d));
		if (d < 0) { write((byte) '-'); d = -d; }
		final var multiplier = POW10_L[limit];
		final var total = Math.round(d * multiplier);
		final var intPart = total / multiplier;
		var fracPart = total % multiplier;
		writeNumber(intPart);
		write((byte) '.');
		if (fracPart == 0) { write((byte) '0'); return null; }
		var tempLimit = multiplier / 10;
		while (tempLimit > 1 && fracPart < tempLimit) { write((byte) '0'); tempLimit /= 10; }
		while (fracPart % 10 == 0) fracPart /= 10;
		writeNumber(fracPart);
		return null;
	}

	void writeTimestampasText(final Temporal t) throws IOException {
		if (t == null) { write(JsonOutputStream.NULL_BYTES); return; }
		final var inst = switch (t) {
		case final Instant        v -> v;
		case final LocalDate      v -> v.atStartOfDay(JsonDateTime.BERLIN).toInstant();
		case final LocalDateTime  v -> v.atZone(JsonDateTime.BERLIN).toInstant();
		case final ZonedDateTime  v -> v.toInstant();
		case final OffsetDateTime v -> v.toInstant();
		default -> throw new IllegalStateException(t.getClass().getName());
		};
		final var utc = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
		mayFlush(26);	// "2000-01-01T00:00:00.001Z"
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

	private void write3(final int val) throws IOException {
		buffer[pos++] = (byte)('0' + (val / 100));
		final var q = val % 100;
		buffer[pos++] = DIGITS[q * 2];
		buffer[pos++] = DIGITS[q * 2 + 1];
	}

	private void write2(final int q) throws IOException {
		buffer[pos++] = DIGITS[q * 2];
		buffer[pos++] = DIGITS[q * 2 + 1];
	}

	void writeNumber(long val) throws IOException {
		if (val == 0) { write((byte)'0'); return; }
		if (val == Long.MIN_VALUE) { write(MIN_LONG); return; }
		if (pos + 20 > buffer.length) flushBuffer();
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

	private void writeCommaIfNeeded() throws IOException { if (pos == 0 || (buffer[pos - 1] != '[' && buffer[pos - 1] != '{')) write((byte)','); }

	private boolean isSkipped(final Object v) {
		if (v == null) return skipNull;
		final Class<?> c = v.getClass(); // Liest den Klass-Pointer in O(1)
		if (c == String.class)  return skipEmpty && ((String) v).isEmpty();
		if (c == Integer.class || c == Long.class || c == Double.class) return false;
		if (c == Boolean.class) return skipFalse && !((Boolean) v);
		if (skipEmpty) {
			if (c == CompactList.class) return ((CompactList<?>) v).size() == 0;
			if (c == CompactMap.class)  return ((CompactMap<?, ?>) v).size() == 0;
			if (c == Object[].class)    return ((Object[]) v).length == 0;
			if (v instanceof final Collection<?> col) return col.isEmpty();
			if (v instanceof final Map<?, ?> m)       return m.isEmpty();
			if (c.isArray())                    return Array.getLength(v) == 0;
			if (v instanceof final Record r)          return engine.ofComplex(r.getClass()).length == 0;
		}
		return false;
	}

	private boolean writePrimitiveInline(final Object val) throws IOException {
		if (val == null) { write(NULL_BYTES); return true; }
		final Class<?> c = val.getClass();
		if (c == String .class) { writeEscapedString ((String ) val); return true; }
		if (c == Integer.class) { writeNumber       (((Integer) val).intValue()); return true; }
		if (c == Long   .class) { writeNumber       (((Long   ) val)); return true; }
		if (c == Boolean.class) { writeBoolean      ((Boolean ) val); return true; }
		if (c == Double .class) { writeDouble       (((Double ) val)); return true; }
		return false;
	}

	private boolean handleSimple(final Object val) throws IOException {
		if (handleCycle(val)) return true;
		var ret = true;
		switch(val) {
		case null                      -> write(NULL_BYTES);
		case final String            t -> writeEscapedString(t);
		case final Integer           t -> writeNumber(t.longValue());
		case final Boolean           t -> write(t ? TRUE_BYTES : FALSE_BYTES);
		case final Long              t -> writeNumber(t);
		case final Double            t -> writeDouble(t);
		case final CompactList<?>    t -> push((byte)'[', TYPE_OBJ_ARRAY, t.getRawData(), null, t.size());
		case final Object[]          t -> push((byte)'[', TYPE_OBJ_ARRAY, t, null, t.length);
		case final CompactMap<?,?>   t -> push((byte)'{', TYPE_COMPACT_MAP, t.getRawData(), null, t.size() * 2);
		case final java.util.List<?> t when t instanceof java.util.RandomAccess -> push((byte)'[', TYPE_LIST, t, null, t.size());
		case final Map<?,?> t -> {
			final var size = t.size();
			if (size == 0) { write(new byte[]{'{','}'}); return true; } // Optionaler Fast-Path
			final var flat = takeArray(size * 2);
			mapDumper.target = flat;
			mapDumper.idx    = 0;
			t.forEach(mapDumper);
			push((byte)'{', TYPE_POOLED_MAP, flat, null, size * 2);
		}

		case final Collection<?>     t -> push((byte)'[', TYPE_COL   , t, t.iterator(),  t.size());
		case final Record            t -> { final var parts = engine.ofComplex(t.getClass()); push((byte)'{', TYPE_RECORD, t, parts, parts.length); }
		case final Short             t -> writeNumber(t.longValue());
		case final Float             t -> writeDouble(t.doubleValue());
		case final BigInteger        t -> writeBaseAscii(t.toString());
		case final Number            t -> writeFractionalLimited(t.toString());
		case final Enum<?>           t -> writeEscapedString(t.name());
		case final Date              t -> timeFormat.writeDate(this, t);
		case final Temporal          t -> timeFormat.writeTemp(this, t);
		case final int    []         t -> { write((byte)'['); if(t.length>0) { writeNumber(t[0]); for(var i=1; i<t.length; i++) { write((byte)','); writeNumber(t[i]); } } write((byte)']'); }
		case final long   []         t -> { write((byte)'['); if(t.length>0) { writeNumber(t[0]); for(var i=1; i<t.length; i++) { write((byte)','); writeNumber(t[i]); } } write((byte)']'); }
		case final double []         t -> { write((byte)'['); if(t.length>0) { writeDouble(t[0]); for(var i=1; i<t.length; i++) { write((byte)','); writeDouble(t[i]); } } write((byte)']'); }
		case final boolean[]         t -> { write((byte)'['); if(t.length>0) { write(t[0] ? TRUE_BYTES : FALSE_BYTES); for(var i=1; i<t.length; i++) { write((byte)','); write(t[i] ? TRUE_BYTES : FALSE_BYTES); } } write((byte)']'); }
		case final short  []         t -> { write((byte)'['); if(t.length>0) { writeNumber(t[0]); for(var i=1; i<t.length; i++) { write((byte)','); writeNumber(t[i]); } } write((byte)']'); }
		case final float  []         t -> { write((byte)'['); if(t.length>0) { writeDouble(t[0]); for(var i=1; i<t.length; i++) { write((byte)','); writeDouble(t[i]); } } write((byte)']'); }
		default                        -> ret = false;
		}
		if(!ret && val.getClass().isArray()) { push((byte)'[', TYPE_ARRAY, val, null, Array.getLength(val)); return true; }
		return ret;
	}

	private void handleValue(final Object val) throws IOException {
		if(null == val) { write(NULL_BYTES); return; }
		if(handleSimple(val)) return;
		final var h = (engine.ofComplex(val.getClass()) instanceof final KeyValueObject[] kv ? kv : DEFAULTOBJ);
		if (h instanceof final KeyValueObject[] parts) push((byte)'{', TYPE_RECORD, val, parts, parts.length);
		else  									       writeEscapedString(val.toString());
	}

	private void writeMapKey(final Object key) throws IOException {
		writeCommaIfNeeded();
		if (key instanceof final String s) writeEscapedString(s);
		else writeEscapedString(String.valueOf(key));
		write((byte)':');
	}
}