package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

sealed class BufferedStream  implements MetaPool permits JsonInputStream {
	private static final int BUFFER_SIZE = 128*1024;
	private static final double[] POW10 = { 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18 };
	private static final long[] POW10_L = {
			1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
			1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L,
			100000000000000L, 1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L
	};

	Map<Object, Object> internPool;
	byte[]                       strBuf          = new byte[1024];
	private byte[][]             stringPoolKeys;
	private String[]             stringPoolVals;
	private int                  stringPoolSize;
	final byte[]                 buffer          = new byte[BUFFER_SIZE];
	private final Object[][]     arrayPool       = new Object[32][];
	private int                  arrayPoolSize   = 0;

	private final ParseContext[][] ctxPools     = new ParseContext[4][32];
	private final int[]            ctxPoolSizes = new int[4];

	private final long[][]         longPool     = new long[32][];
	private int                    longPoolSize = 0;

	protected InternalEngine engine;
	InputStream in;
	int         pos               ;
	int         limit             ;
	int         depth             ;
	int         maxCollectionSize ;
	int         maxStringLength   ;
	int         maxDepth          ;
	int     escapedParsedLen;
	boolean escapedIsAscii  ;

	private final ObjectMeta[] dynamicMetaCache = new ObjectMeta[16];
	private int dynamicMetaCount = 0;

	ObjectMeta getDynamicMeta(final Class<?> compType, final int targetMetaType) {
		final var searchType = compType == null ? Object.class : compType;
		for (var i = 0; i < dynamicMetaCount; i++) {
			final var m = dynamicMetaCache[i];
			if (m.metaType == targetMetaType && m.type(0) == searchType) return m;
		}
		final var m = new ObjectMeta(engine, searchType, targetMetaType);
		if (dynamicMetaCount < 32) dynamicMetaCache[dynamicMetaCount++] = m;
		return m;
	}

	final void init(final InputStream i, final InternalEngine c) {
		final var cfg          = c.config();
		this.engine            = c;
		this.maxDepth          = cfg.maxDepth();
		this.maxCollectionSize = cfg.maxCollectionSize();
		this.maxStringLength   = cfg.maxStringLength();
		this.maxCollectionSize = cfg.maxCollectionSize();
		this.in                = i;
		this.pos               = 0;
		this.limit             = 0;
		this.depth             = -1;
		onceInternal.clear();
		if (cfg.enableDeduplication()) {
			if (this.internPool     == null) {
				this.internPool     = new java.util.HashMap<>();
				this.stringPoolKeys = new byte[1024][];
				this.stringPoolVals = new String[1024];
			} else {
				this.internPool.clear();
				java.util.Arrays.fill(this.stringPoolKeys, null);
				java.util.Arrays.fill(this.stringPoolVals, null);
			}
			this.stringPoolSize = 0;
		} else {
			this.internPool     = null;
			this.stringPoolKeys = null;
			this.stringPoolVals = null;
		}
	}

	private static int getBucket(final int capacity) {
		if (capacity <= 16)  return 0;
		if (capacity <= 64)  return 1;
		if (capacity <= 256) return 2;
		return 3;
	}

	@Override public long[] takeLongArray(final int minCapacity) {
		if (longPoolSize > 0) {
			final var arr = longPool[--longPoolSize];
			if (arr.length >= minCapacity) return arr;
		}
		return new long[Math.max(16, minCapacity)];
	}

	@Override public void returnLongArray(final long[] arr) {
		if (arr.length > 2048) return; // Zu große Arrays dem GC überlassen
		if (longPoolSize < longPool.length) longPool[longPoolSize++] = arr;
	}

	@Override public ParseContext takeContext(final int minCapacity) {
		final var b = getBucket(minCapacity);
		if (ctxPoolSizes[b] > 0) {
			final var ctx = ctxPools[b][--ctxPoolSizes[b]];
			if (ctx.objs == null || ctx.objs.length < minCapacity) {
				if (ctx.objs != null) returnArray(ctx.objs);
				if (ctx.prims != null) returnLongArray(ctx.prims);
				ctx.objs = takeArray(minCapacity);
				ctx.prims = takeLongArray(ctx.objs.length); // GC Zero!
			}
			ctx.cnt = 0;
			ctx.currentKey = null;
			return ctx;
		}
		final var ctx = new ParseContext();
		ctx.objs = takeArray(Math.max(16, minCapacity));
		ctx.prims = takeLongArray(ctx.objs.length); // GC Zero!
		return ctx;
	}

	@Override public void returnContext(final ParseContext ctx) {
		if (ctx.objs == null) return;
		final var b = getBucket(ctx.objs.length); // Nach ECHTER Array-Länge einsortieren
		if (ctxPoolSizes[b] < 32) {
			if (ctx.cnt > 0) {
				Arrays.fill(ctx.objs, 0, ctx.cnt, null);
				Arrays.fill(ctx.prims, 0, ctx.cnt, 0L);
			}
			ctxPools[b][ctxPoolSizes[b]++] = ctx;
		} else {
			returnArray(ctx.objs);
			returnLongArray(ctx.prims);
		}
	}

	@Override public Object[] takeArray(final int minCapacity) {
		if (arrayPoolSize > 0) {
			final var arr = arrayPool[--arrayPoolSize];
			if (arr.length >= minCapacity) return arr;
		}
		return new Object[Math.max(16, minCapacity)];
	}

	@Override public void     returnArray(final Object[] arr) {
		if (arr.length > 1024) return; // Large arrays for GC to avoid large Arrays.fill
		if (arrayPoolSize < arrayPool.length) { Arrays.fill(arr, null); arrayPool[arrayPoolSize++] = arr; }
	}

	@Override public InternalEngine engine() { return engine; }

	final HashSet<Object> onceInternal = new HashSet<>();

	final void ensure(final int n) throws IOException {
		final var remaining = limit - pos;
		if (remaining >= n || limit < 0) return;
		if (remaining > 0 && pos > 0) System.arraycopy(buffer, pos, buffer, 0, remaining);
		pos = 0;
		limit = remaining;
		while (limit < n) {
			final var count = in.read(buffer, limit, buffer.length - limit);
			if (count == -1) break;
			limit += count;
		}
	}

	@Override
	public Object deduplicate(final Object value) {
		if (internPool == null || value == null) return value;
		final var existing = internPool.putIfAbsent(value, value);
		return existing != null ? existing : value;
	}

	private long    numberVal  = 0L;
	private int     totalExp   = 0;
	private boolean isNegative = false;
	private boolean isFloat    = false;

	private final void parseNumberCore() throws IOException {
		var l_intDigitCount = 0;
		var l_fracDigits    = 0;
		var l_numberVal     = 0L;
		var l_isNegative    = false;
		var l_parsedExp     = 0;
		var l_virtualExp    = 0;
		var l_expNeg        = false;
		// 1. Initial bounds check and sign
		ensure(64);
		final var limitSafe = limit; // Local copy for performance

		if (pos < limitSafe && buffer[pos] == '-') {
			l_isNegative = true;
			pos++;
		}

		// 2. MICRO-LOOP 1: Integer Part (Before the dot)
		// The CPU loves this: No state checks, just pure arithmetic.
		while (pos < limitSafe) {
			final var d = buffer[pos] - '0';
			// Unsigned check: catches everything outside '0'-'9' (including '.' and 'e')
			if (d < 0 || d > 9) break;

			if (++l_intDigitCount <= 18) {
				l_numberVal = l_numberVal * 10L + d;
			} else {
				// Prevent long overflow, shift the virtual decimal point
				l_virtualExp++;
			}
			pos++;
		}

		if (l_intDigitCount == 0) throw new IllegalStateException("Missing integer digits");

		// 3. MICRO-LOOP 2: Fractional Part (After the dot)
		if (pos < limitSafe && buffer[pos] == '.') {
			pos++; // Skip the dot
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;

				if (l_intDigitCount + l_fracDigits < 18) {
					l_fracDigits++;
					l_numberVal = l_numberVal * 10L + d;
				}
				// Extra digits beyond 18 total significance are silently ignored
				pos++;
			}
			if (l_fracDigits == 0 && l_intDigitCount <= 18) {
				// Edge case: "123." without trailing digits
				throw new IllegalStateException("Missing fractional digits");
			}
		}

		// 4. MICRO-LOOP 3: Exponent Part ('e' or 'E')
		if (pos < limitSafe && (buffer[pos] == 'e' || buffer[pos] == 'E')) {
			pos++; // Skip 'e'
			if (pos < limitSafe) {
				final var sign = buffer[pos];
				if (sign == '-') { l_expNeg = true; pos++; }
				else if (sign == '+') { pos++; }
			}
			var expDigits = 0;
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				l_parsedExp = l_parsedExp * 10 + d;
				expDigits++;
				pos++;
			}
			if (expDigits == 0) throw new IllegalStateException("Missing exponent digits");
		}
		this.totalExp = (l_expNeg ? -l_parsedExp : l_parsedExp) + l_virtualExp - l_fracDigits;

		// Normalize: Fast-path for positive exponents.
		// If the padded number securely fits into 18 digits, apply the exponent directly.
		final var sigDigits = l_intDigitCount + l_fracDigits;
		if (this.totalExp > 0 && sigDigits + this.totalExp <= 18) {
			l_numberVal *= POW10_L[this.totalExp];
			this.totalExp = 0; // Exponent is completely absorbed
		}

		this.numberVal  = l_numberVal;
		this.isNegative = l_isNegative;

		// The value is strictly a float ONLY if an unresolved exponent remains
		this.isFloat    = (this.totalExp != 0);
	}

	// ---------------------------------------------------------
	// Math calculation helpers to prevent double-parsing
	// ---------------------------------------------------------

	private double computeDoubleValue() {
		double d = numberVal;
		if (totalExp != 0) {
			if (totalExp > 0 && totalExp < POW10.length) {
				d *= POW10[totalExp];
			} else if (totalExp < 0 && -totalExp < POW10.length) {
				d /= POW10[-totalExp];
			} else {
				d *= Math.pow(10, totalExp);
			}
		}
		return isNegative ? -d : d;
	}

	private long computeLongValue() {
		var v = numberVal;
		if (totalExp != 0) {
			// Using POW10_L avoids double-to-long casting overhead
			if (totalExp > 0 && totalExp < POW10_L.length) {
				v *= POW10_L[totalExp];
			} else if (totalExp < 0 && -totalExp < POW10_L.length) {
				v /= POW10_L[-totalExp];
			} else {
				v = (long) (v * Math.pow(10, totalExp));
			}
		}
		return isNegative ? -v : v;
	}

	// ---------------------------------------------------------
	// Public parser methods
	// ---------------------------------------------------------

	final double parseDoublePrimitive() throws IOException {
		parseNumberCore();
		return computeDoubleValue();
	}

	final long parseLongPrimitive() throws IOException {
		parseNumberCore();
		return computeLongValue();
	}

	final Object parseNumber(final Class<?> targetType) throws IOException {
		// Parse the digits from the buffer exactly ONCE
		parseNumberCore();

		if (targetType == boolean.class || targetType == Boolean.class) {
			return numberVal != 0 ? Boolean.TRUE : Boolean.FALSE;
		}

		final var isGeneric = targetType == Object.class || targetType == Number.class;

		// Handle floating point targets and explicit JSON floats
		if (isFloat || targetType == double.class || targetType == Double.class || targetType == float.class || targetType == Float.class) {
			final var d = computeDoubleValue();
			if (targetType == float.class || targetType == Float.class) return (float) d;
			return d;
		}

		// Handle integer targets
		final var v = computeLongValue();

		// Auto-downcast to integer if it safely fits into 32-bit limits
		final var wantInt = targetType == int.class || targetType == Integer.class || (isGeneric && v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE);
		if (wantInt) return (int) v;

		return v;
	}

	final void parseNumericPrimitive(final ObjectMeta meta, final Object ctx, final int targetIdx, final Class<?> targetType) throws IOException {
		parseNumberCore();
		if (targetType == double.class || targetType == float.class || (isFloat && targetType != long.class && targetType != int.class)) {
			meta.setDouble(this, ctx, targetIdx, computeDoubleValue());
		} else {
			meta.setLong(this, ctx, targetIdx, computeLongValue());
		}
	}

	final Object parseNull() throws IOException {
		ensure(4);
		if (limit - pos < 4 || buffer[pos+1] != 'u' || buffer[pos+2] != 'l' || buffer[pos+3] != 'l') throw new IllegalStateException();
		pos += 4;
		return null;
	}

	private static final int STR_HARD_LIMIT = Integer.MAX_VALUE - 8;

	final void expandStrBuf(final int minCapacity) {
		if (minCapacity < 0) throw new IllegalStateException();
		final var curCapacity = strBuf.length;
		if(curCapacity >= minCapacity) return;
		// engine limit check
		if (minCapacity > maxStringLength) throw new IllegalStateException();
		int newCapacity;
		// Bitshift-Overflow safe increase
		if (curCapacity > 0x3FFFFFFF) {
			if (minCapacity > STR_HARD_LIMIT) throw new OutOfMemoryError();
			newCapacity = STR_HARD_LIMIT;
		} else {
			newCapacity = curCapacity << 1;
			if (newCapacity < minCapacity) newCapacity = minCapacity;
		}
		strBuf = Arrays.copyOf(strBuf, newCapacity);
	}

	final boolean parseTruePrimitive() throws IOException {
		ensure(4);
		if(limit - pos < 4
				|| buffer[pos  ] != 't'
				|| buffer[pos+1] != 'r'
				|| buffer[pos+2] != 'u'
				|| buffer[pos+3] != 'e') throw new IllegalStateException();
		pos += 4;
		return true;
	}

	final boolean parseFalsePrimitive() throws IOException {
		ensure(5);
		if(limit - pos < 5 || buffer[pos] != 'f' || buffer[pos+1] != 'a'
				|| buffer[pos+2] != 'l' || buffer[pos+3] != 's' || buffer[pos+4] != 'e') throw new IllegalStateException();
		pos += 5;
		return false;
	}

	final boolean parseBooleanPrimitive() throws IOException {
		ensure(4);
		if(limit - pos < 4) throw new IllegalStateException();
		if(            buffer[pos  ] == 't') {
			if (       buffer[pos+1] != 'r'
					|| buffer[pos+2] != 'u'
					|| buffer[pos+3] != 'e') throw new IllegalStateException();
			pos += 4;
			return true;
		}
		if(buffer[pos] != 'f' || buffer[pos+1] != 'a' || buffer[pos+2] != 'l' || buffer[pos+3] != 's')  throw new IllegalStateException();
		ensure(5);
		if (limit - pos < 5  || buffer[pos+4] != 'e') throw new IllegalStateException();
		pos += 5;
		return false;
	}

	final Boolean parseTrue() throws IOException {
		ensure(4);
		if (limit - pos < 4 || buffer[pos+1] != 'r' || buffer[pos+2] != 'u' || buffer[pos+3] != 'e') throw new IllegalStateException();
		pos += 4;
		return Boolean.TRUE;
	}

	final Boolean parseFalse() throws IOException {
		ensure(5);
		if (limit - pos < 5 || buffer[pos+1] != 'a' || buffer[pos+2] != 'l' || buffer[pos+3] != 's' || buffer[pos+4] != 'e') throw new IllegalStateException();
		pos += 5;
		return Boolean.FALSE;
	}

	private int parseHex4() throws IOException {
		if (limit - pos < 4) ensure(4);
		if (limit - pos < 4) throw new IllegalStateException();
		var cp = 0;
		for (var i = 0; i < 4; i++) {
			final var b = buffer[pos++];
			final int digit;
			if (b >= '0' && b <= '9') digit = b - '0';
			else if (b >= 'a' && b <= 'f') digit = b - 'a' + 10;
			else if (b >= 'A' && b <= 'F') digit = b - 'A' + 10;
			else throw new IllegalStateException();
			cp = (cp << 4) | digit;
		}
		return cp;
	}

	void parseU(int dstLen) throws IOException {
		final var buf = this.buffer;
		var isAscii = true   ;
		var cp = BufferedStream.this.parseHex4();
		if (cp >= 0xD800 && cp <= 0xDBFF) {
			if (limit - pos < 6) ensure(6);
			if (limit - pos >= 6 && buf[pos] == '\\' && buf[pos + 1] == 'u') {
				pos += 2;
				cp = Character.toCodePoint((char) cp, (char) parseHex4());
			}
		}
		if (dstLen + 4 > strBuf.length) expandStrBuf(dstLen + 4);
		if(cp < 0x80) {
			isAscii = false;
			if(cp < 0x800) {
				strBuf[dstLen++] = (byte) (0xC0 | (cp >> 6));
			} else {
				if(cp < 0x10000) {
					strBuf[dstLen++] = (byte) (0xE0 | (cp >> 12));
				} else {
					strBuf[dstLen++] = (byte) (0xF0 | (cp >> 18));
					strBuf[dstLen++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
				}
				strBuf[dstLen++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
			}
			strBuf[dstLen++] = (byte) (0x80 | (cp & 0x3F));
		}
		this.escapedIsAscii   = isAscii;
		this.escapedParsedLen = dstLen;
	}

	final int parseStringKeyAsIndex(final Object context, final ObjectMeta meta) throws IOException {
		var lPos = pos;
		final var start = lPos;
		final var buf = buffer;
		var hash = 0; // Hash wird on-the-fly berechnet

		while (lPos < limit) {
			final var b = buf[lPos];
			if (b == '"') {
				final var len = lPos - start;
				pos = lPos + 1;
				// Zuerst Byte-Lookup in der Tabelle (für POJOs)
				final var idx = meta.prepareKey(hash, buf, start, len);
				if (idx != -1) return idx;

				// Nur wenn unbekannt oder MAP, String aus dem Pool holen
				final var key = internBytes(buf, start, len, hash, true);
				return meta.prepareKey(context, key);
			}
			if (b == '\\' || b < 0) break;
			hash = 31 * hash + b; // Fused Hashing
			lPos++;
		}
		// Slow path handles escapes and UTF-8 decoding.
		final var key = parseStringSlow(start, lPos, true);
		return meta.prepareKey(context, key);
	}

	final String parseStringValue() throws IOException {
		var       lPos   = pos;
		final var lLimit = limit;
		final var start  = lPos;
		final var buf    = buffer;
		var       hash   = 0; // Hash wird on-the-fly berechnet

		// MICRO-LOOP: Pure ASCII fast-path
		while (lPos < lLimit) {
			final var b = buf[lPos];
			if (b == '"') {
				pos = lPos + 1;
				// FIX: Nutzt jetzt den echten Hash und dedupliziert Werte extrem aggressiv!
				return internBytes(buf, start, lPos - start, hash, true);
			}
			if (b == '\\' || b < 0) break;
			hash = 31 * hash + b; // Fused Hashing
			lPos++;
		}
		return parseStringSlow(start, lPos, true);
	}

	private String parseStringSlow(final int start, final int lPos, boolean isAscii) throws IOException {
		var parsedLen = lPos - start;
		if (parsedLen > strBuf.length) expandStrBuf(parsedLen);
		if (parsedLen > 0) System.arraycopy(buffer, start, strBuf, 0, parsedLen);
		this.pos = lPos;

		while (true) {
			while (pos < limit) {
				final var b = buffer[pos];
				if (b < 0) isAscii = false;
				if (b == '"' || b == '\\') break;
				if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
				strBuf[parsedLen++] = b;
				pos++;
			}
			if (pos >= limit) {
				ensure(1);
				if (pos >= limit) throw new IllegalStateException();
				continue;
			}
			var b = buffer[pos++];
			if (b == '"') break;
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			b = buffer[pos++];
			switch (b) {
			case '"', '\\', '/' -> { }
			case 'b' -> b = '\b';
			case 'f' -> b = '\f';
			case 'n' -> b = '\n';
			case 'r' -> b = '\r';
			case 't' -> b = '\t';
			case 'u' -> {
				parseU(parsedLen);
				parsedLen  = escapedParsedLen;
				isAscii &= escapedIsAscii;
			}
			default -> throw new IllegalStateException();
			}

			if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
			strBuf[parsedLen++] = b;
			if (b < 0) isAscii = false;
		}

		// Hash nach Escaping sauber über den temporären Buffer berechnen
		var hash = 0;
		for (var i = 0; i < parsedLen; i++) hash = 31 * hash + strBuf[i];
		return internBytes(strBuf, 0, parsedLen, hash, isAscii);
	}

	@Override
	public final String internBytes(final byte[] src, final int start, final int len, final int hash, final boolean isAscii) {
		if (stringPoolKeys == null) return new String(src, start, len, isAscii ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);

		final var keys = this.stringPoolKeys;
		final var mask = keys.length - 1;
		var idx = hash & mask;

		// Hyper-optimierter Linear Probe ohne instanceof
		while (true) {
			final var k = keys[idx];
			if (k == null) break;
			if (k.length == len && Arrays.equals(k, 0, len, src, start, start + len)) {
				return stringPoolVals[idx];
			}
			idx = (idx + 1) & mask;
		}
		return internBytesSlow(src, start, len, isAscii, mask, idx);
	}

	private String internBytesSlow(final byte[] src, final int start, final int len, final boolean isAscii, int mask, final int idx) {
		if (stringPoolSize >= maxCollectionSize) return new String(src, start, len, isAscii ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);

		final var s = new String(src, start, len, isAscii ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
		stringPoolKeys[idx] = java.util.Arrays.copyOfRange(src, start, start + len);
		stringPoolVals[idx] = s;

		if (++stringPoolSize > stringPoolKeys.length * 0.75f) {
			final var oldK = stringPoolKeys;
			final var oldV = stringPoolVals;
			stringPoolKeys = new byte[oldK.length << 1][];
			stringPoolVals = new String[oldV.length << 1];
			mask = stringPoolKeys.length - 1;
			for (var i = 0; i < oldK.length; i++) {
				final var k = oldK[i];
				if (k != null) {
					var h = 0;
					for (final byte b : k) h = 31 * h + b;
					var newIdx = h & mask;
					while (stringPoolKeys[newIdx] != null) newIdx = (newIdx + 1) & mask;
					stringPoolKeys[newIdx] = k;
					stringPoolVals[newIdx] = oldV[i];
				}
			}
		}
		return s;
	}

	// Bit 9 (\t), 10 (\n), 13 (\r), 32 (Space)
	private static final long WHITESPACE_MASK = (1L << 9) | (1L << 10) | (1L << 13) | (1L << 32);

	void skipWhitespace() throws IOException {
		var p = pos;
		final var l = limit;
		final var buf = buffer;
		while (p < l) {
			final var b = buf[p];
			// Fast check  32? Negativ (UTF-8)? Nicht in der Maske? -> Abbruch.
			if (b > 0x20 || b < 0 || (WHITESPACE_MASK & (1L << b)) == 0) {
				pos = p;
				return;
			}
			p++;
		}
		pos = p;
		skipWhitespaceSlow();
	}

	private void skipWhitespaceSlow() throws IOException {
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) return; }
			final var b = buffer[pos];
			if (b > 0x20) return;
			if (b != 0x20 && b != 0x09 && b != 0x0A && b != 0x0D) break;
			pos++;
		}
	}

	final void skipValue() throws IOException {
		final var b = peek();
		switch (b) {
		case '{' -> { read(); for (var curDepth = 1; curDepth > 0; ) {
			final var c = read();
			switch (c) {
			case '"': skipString(); break;
			case '{': curDepth++; break;
			case '}': curDepth--; break;
			default: break;
			}
		}
		}
		case '[' -> { read(); for (var curDepth = 1; curDepth > 0; ) {
			final var c = read();
			switch (c) {
			case '"': skipString(); break;
			case '[': curDepth++; break;
			case ']': curDepth--; break;
			default: break;
			}
		}
		}
		case '"' -> { read(); skipString(); }
		case 't', 'f', 'n' -> { final var len = (b == 'f' ? 5 : 4); ensure(len); pos += len; }
		default -> {
			while (true) {
				if (pos >= limit) { ensure(1); if (pos >= limit) return; }
				final var c = buffer[pos];
				if (c <= ' ' || c == ',' || c == '}' || c == ']') return;
				pos++;
			}
		}
		}
	}

	final void skipString() throws IOException {
		while (true) {
			if (pos >= limit) ensure(1);
			final var c = buffer[pos++];
			if (c == '"') return;
			if (c == '\\') {
				if (pos >= limit) ensure(1);
				pos++;
			}
		}
	}

	void expect(final byte b) throws IOException { final var r = read(); if (r != b) throw new IllegalStateException("expect("+(char)b+")!="+(char)r); }

	byte peek() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos];
	}

	byte read() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos++];
	}
}