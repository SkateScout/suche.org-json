package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.suche.annotation.NonNull;

sealed abstract class BufferedStream  implements MetaPool permits JsonInputStream {
	private static final int        BUFFER_SIZE = 128*1024;
	private static final double[]   POW10 = { 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18 };
	private static final long[]     POW10_L = {
			1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
			1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L,
			100000000000000L, 1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L
	};
	private static final int        POOL_SIZE = 128;
	private static final int        STRING_POOL_SIZE = 4096;
	// Little-Endian 32-Bit Konstanten für "true", "fals" und "null"
	private static final int TRUE_MAGIC  = 0x65757274; // 'e'|'u'|'r'|'t'
	private static final int FALSE_MAGIC = 0x736C6166; // 's'|'l'|'a'|'f'
	private static final int NULL_MAGIC  = 0x6C6C756E; // 'l'|'l'|'u'|'n'

	// FNV-1a 64-bit prime
	private static final long HASH_PRIME = 0x100000001b3L;
	// FNV-1a offset basis
	private static final long HASH_BASIS = 0xcbf29ce484222325L;

	// Maskiert die ungenutzten Bytes im Tail-Word aus.
	// Da offset maximal 7 ist, reicht ein Array der Größe 8.
	private static final long[] TAIL_MASKS = {
			0x0000000000000000L,
			0x00000000000000FFL,
			0x000000000000FFFFL,
			0x0000000000FFFFFFL,
			0x00000000FFFFFFFFL,
			0x000000FFFFFFFFFFL,
			0x0000FFFFFFFFFFFFL,
			0x00FFFFFFFFFFFFFFL
	};

	Map<Object, Object> internPool;
	byte[]                       strBuf          = new byte[1024];
	private byte[][]             stringPoolKeys;
	private String[]             stringPoolVals;
	private int                  stringPoolSize;
	final byte[]                 buffer          = new byte[BUFFER_SIZE];
	private boolean useFastString = true;

	// --- NEU: DIE 4 SLAB BUCKETS (0: <=16, 1: <=128, 2: <=512, 3: <=2048) ---
	private final Object[][][] arraySlabs    = new Object[4][32][];
	private final int[]        arraySlabSize = new int[4];

	private final long[][][]   longSlabs     = new long[4][32][];
	private final int[]        longSlabSize  = new int[4];

	// Branchless-optimierte Bucket-Berechnung
	private static int getBucket(final int capacity) {
		if (capacity <=   16) return 0;
		if (capacity <=  128) return 1;
		if (capacity <=  512) return 2;
		if (capacity <= 2048) return 3;
		return -1; // Zu groß, geht an den GC
	}

	private final ParseContext[] ctxPool     = new ParseContext[POOL_SIZE];
	private int                  ctxPoolSize = 0;

	protected InternalEngine engine;
	InputStream in;
	int         pos               ;
	int         limit             ;
	int         depth             ;
	int         maxCollectionSize ;
	int         maxStringLength   ;
	int         maxDepth          ;
	int         escapedParsedLen  ;
	int         maxRecursiveDepth ;
	boolean     escapedIsAscii  ;
	ObjectMeta  genericCollectionMeta;
	ObjectMeta  genericArrayMeta     ;
	ObjectMeta  genericSetMeta       ;

	final Object[] onceInternal = new Object[32];
	int            onceInternalSize = 0;

	static void throwInvalid(final String mesg) { throw new IllegalStateException(mesg); }

	final void init(final InputStream i, final InternalEngine c) {
		final var cfg          = c.config();
		this.maxDepth          = cfg.maxDepth();
		this.maxCollectionSize = cfg.maxCollectionSize();
		this.maxStringLength   = cfg.maxStringLength();
		this.maxRecursiveDepth = c.maxRecursiveDepth();

		this.engine            = c;
		this.in                = i;
		this.pos               = 0;
		this.limit             = 0;
		this.depth             = -1;
		onceInternalSize = 0;
		if (cfg.enableDeduplication()) {
			if (this.internPool     == null) {
				this.internPool     = new java.util.HashMap<>();
				this.stringPoolKeys = new byte  [STRING_POOL_SIZE][];
				this.stringPoolVals = new String[STRING_POOL_SIZE];
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

	@Override public Object[] takeArray(final int exactCapacity) {
		final var b = getBucket(exactCapacity);
		if (b >= 0) {
			final var size = arraySlabSize[b];
			if (size > 0) {
				final var lastIdx = size - 1;
				arraySlabSize[b] = lastIdx;
				final var arr = arraySlabs[b][lastIdx];
				arraySlabs[b][lastIdx] = null; // Memory-Leak Schutz
				return arr;
			}
			// Bucket ist leer -> Allokiere direkt die Bucket-Maximalgröße!
			final var slabCap = (b == 0) ? 16 : (b == 1) ? 128 : (b == 2) ? 512 : 2048;
			return new Object[Math.max(exactCapacity, slabCap)];
		}
		return new Object[exactCapacity];
	}

	@Override public long[] takeLongArray(final int exactCapacity) {
		final var b = getBucket(exactCapacity);
		if (b >= 0) {
			final var size = longSlabSize[b];
			if (size > 0) {
				final var lastIdx = size - 1;
				longSlabSize[b] = lastIdx;
				final var arr = longSlabs[b][lastIdx];
				longSlabs[b][lastIdx] = null;
				return arr;
			}
			final var slabCap = (b == 0) ? 16 : (b == 1) ? 128 : (b == 2) ? 512 : 2048;
			return new long[Math.max(exactCapacity, slabCap)];
		}
		return new long[exactCapacity];
	}

	// WICHTIG: Die Signatur im MetaPool Interface muss lauten: void returnArray(Object[] arr, int useCount);
	@Override public void returnArray(final Object[] arr, final int useCount) {
		final var b = getBucket(arr.length);
		if (b >= 0) {
			final var size = arraySlabSize[b];
			if (size < 32) {
				// Nutzt native C2 Intrinsics: Löscht NUR die tatsächlich benutzten Elemente!
				if (useCount > 0) Arrays.fill(arr, 0, Math.min(useCount, arr.length), null);
				arraySlabs[b][size] = arr;
				arraySlabSize[b] = size + 1;
			}
		}
	}

	@Override public void returnLongArray(final long[] arr) {
		final var b = getBucket(arr.length);
		if (b >= 0) {
			final var size = longSlabSize[b];
			if (size < 32) {
				// Primitives MÜSSEN für den GC nicht gelöscht werden -> 0 Overhead
				longSlabs[b][size] = arr;
				longSlabSize[b] = size + 1;
			}
		}
	}

	@Override public ParseContext takeContext(final boolean map) {
		// Wir ignorieren die Capacity hier, da Arrays lazy in ObjectMeta allokiert werden
		final ParseContext ctx;
		if (ctxPoolSize > 0) {
			ctx = ctxPool[--ctxPoolSize];
		} else {
			ctx = new ParseContext();
		}
		ctx.cnt        = 0;
		ctx.singleType = MetaPool.T_EMPTY;
		ctx.currentKey = null;
		ctx.objs       = null;
		ctx.prims      = null;
		ctx.map        = map ? 1 : 0;
		return ctx;
	}

	@Override public void returnContext(final @NonNull ParseContext ctx) {
		if (ctx.objs != null) {
			returnArray(ctx.objs, ctx.cnt<<ctx.map);
			ctx.objs = null;
		}
		if (ctx.prims != null) {
			returnLongArray(ctx.prims);
			ctx.prims = null;
		}
		if (ctxPoolSize < ctxPool.length) ctxPool[ctxPoolSize++] = ctx;
	}

	String newString(final byte[] src, final int start, final int len, final boolean isAscii) {
		if(useFastString && isAscii && null != JSONStringAddOpens.STRING_SECRET_CONSTRUCTOR) {
			final var exactBuf = Arrays.copyOfRange(src, start, start + len);
			try {
				final var coder = (byte) (isAscii ? 0 : 1);
				return (String) JSONStringAddOpens.STRING_SECRET_CONSTRUCTOR.invokeExact(exactBuf, coder);
			} catch (final Throwable e) {
				e.printStackTrace();
				useFastString = false;
			}
		}
		return new String(src, start, len, isAscii ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
	}

	@Override
	public final String internBytes(final byte[] src, final int start, final int len, final int hash, final boolean isAscii) {
		// Strings over 36 Zeichen (Text, long URLs) are not pooled.
		if (stringPoolKeys == null || len > 36) return newString(src, start, len, isAscii);
		final var keys = this.stringPoolKeys;
		final var mask = keys.length - 1;
		var idx = hash & mask;
		while (true) {
			final var k = keys[idx];
			if (k == null) break;
			if (k.length == len && Arrays.equals(k, 0, len, src, start, start + len)) return stringPoolVals[idx];
			idx = (idx + 1) & mask;
		}
		return internBytesSlow(src, start, len, isAscii, mask, idx);
	}

	private String internBytesSlow(final byte[] src, final int start, final int len, final boolean isAscii, int mask, final int idx) {
		if (stringPoolSize >= maxCollectionSize) return newString(src, start, len, isAscii);
		final var s = newString(src, start, len, isAscii);
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

	@Override public InternalEngine engine() { return engine; }

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
		var lIntDigitCount = 0;
		var lFracDigits    = 0;
		var lNumberVal     = 0L;
		var lIsNegative    = false;
		var lParsedExp     = 0;
		var lVirtualExp    = 0;
		var lExpNeg        = false;
		// 1. Initial bounds check and sign
		ensure(64);
		final var limitSafe = limit; // Local copy for performance
		if (pos < limitSafe && buffer[pos] == '-') {
			lIsNegative = true;
			pos++;
		}
		// Integer Part (Before the dot)
		while (pos < limitSafe) {
			final var d = buffer[pos] - '0';
			// Unsigned check: catches everything outside '0'-'9' (including '.' and 'e')
			if (d < 0 || d > 9) break;
			if (++lIntDigitCount <= 18) {
				lNumberVal = lNumberVal * 10L + d;
			} else {
				// Prevent long overflow, shift the virtual decimal point
				lVirtualExp++;
			}
			pos++;
		}
		if (lIntDigitCount == 0) throwInvalid("Missing integer digits");
		// Fractional Part (After the dot)
		if (pos < limitSafe && buffer[pos] == '.') {
			pos++; // Skip the dot
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				if (lIntDigitCount + lFracDigits < 18) {
					lFracDigits++;
					lNumberVal = lNumberVal * 10L + d;
				}
				// Extra digits beyond 18 total significance are silently ignored
				pos++;
			}
			if (lFracDigits == 0 && lIntDigitCount <= 18) {
				// Edge case: "123." without trailing digits
				throwInvalid("Missing fractional digits");
			}
		}

		// Exponent Part ('e' or 'E')
		if (pos < limitSafe && (buffer[pos] == 'e' || buffer[pos] == 'E')) {
			pos++; // Skip 'e'
			if (pos < limitSafe) {
				final var sign = buffer[pos];
				if (sign == '-') { lExpNeg = true; pos++; }
				else if (sign == '+') { pos++; }
			}
			var expDigits = 0;
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				lParsedExp = lParsedExp * 10 + d;
				expDigits++;
				pos++;
			}
			if (expDigits == 0) throwInvalid("Missing exponent digits");
		}
		this.totalExp = (lExpNeg ? -lParsedExp : lParsedExp) + lVirtualExp - lFracDigits;

		// Normalize: Fast-path for positive exponents.
		// If the padded number securely fits into 18 digits, apply the exponent directly.
		final var sigDigits = lIntDigitCount + lFracDigits;
		if (this.totalExp > 0 && sigDigits + this.totalExp <= 18) {
			lNumberVal *= POW10_L[this.totalExp];
			this.totalExp = 0; // Exponent is completely absorbed
		}

		this.numberVal  = lNumberVal;
		this.isNegative = lIsNegative;

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

	// TODO replace targetType with descId this allow easy check for primitve via bit op
	final void parseNumericPrimitive(final ObjectMeta meta, final Object ctx, final int targetIdx, final Class<?> targetType) throws IOException {
		parseNumberCore();
		if(null == targetType) { // TODO Marker that our Map Implementation can handle both primitive types
			if(isFloat) meta.setDouble(this, ctx, targetIdx, computeDoubleValue());
			else        meta.setLong  (this, ctx, targetIdx, computeLongValue  ());
		} else if (targetType == double.class || targetType == float.class || (isFloat && targetType != long.class && targetType != int.class)) {
			meta.setDouble(this, ctx, targetIdx, computeDoubleValue());
		} else {
			meta.setLong(this, ctx, targetIdx, computeLongValue());
		}
	}

	final Object parseNull() throws IOException {
		if(limit - pos < 4) ensure(4);
		if (limit - pos < 4 || (int) JSONStringAddOpens.INT_VIEW.get(buffer, pos) != NULL_MAGIC) throwInvalid("Not expected NULL");
		pos += 4;
		return null;
	}

	private static final int STR_HARD_LIMIT = Integer.MAX_VALUE - 8;

	final void expandStrBuf(final int minCapacity) {
		if (minCapacity < 0) throwInvalid("MinCapactiy negativ");
		final var curCapacity = strBuf.length;
		if(curCapacity >= minCapacity) return;
		// engine limit check
		if (minCapacity > maxStringLength) throwInvalid("String size limit reached.1");
		int newCapacity;
		// Bitshift-Overflow safe increase
		if (curCapacity > 0x3FFFFFFF) {
			if (minCapacity > STR_HARD_LIMIT) throwInvalid("String size limit reached");
			newCapacity = STR_HARD_LIMIT;
		} else {
			newCapacity = curCapacity << 1;
			if (newCapacity < minCapacity) newCapacity = minCapacity;
		}
		strBuf = Arrays.copyOf(strBuf, newCapacity);
	}

	final boolean parseTrueOrFalse(final boolean expected) throws IOException {
		final var l = expected ? 4 : 5;
		if(limit - pos < l) ensure(l);
		if (limit - pos >= l) {
			if (expected) {
				if    ((int) JSONStringAddOpens.INT_VIEW.get(buffer, pos) == TRUE_MAGIC                         ) { pos += l; return expected; }
			} else if ((int) JSONStringAddOpens.INT_VIEW.get(buffer, pos) == FALSE_MAGIC && buffer[pos+4] == 'e') { pos += l; return expected; }
		}
		throwInvalid("Bollean not true or false");
		return false; // Will never called
	}

	final boolean parseBooleanPrimitive() throws IOException {
		ensure(4);
		if(limit - pos < 4) throwInvalid("Not expected end instead of BOOLEAN");
		if(            buffer[pos  ] == 't') {
			if (       buffer[pos+1] != 'r'
					|| buffer[pos+2] != 'u'
					|| buffer[pos+3] != 'e') throwInvalid("Not expected TRUE.1");
			pos += 4;
			return true;
		}
		if(buffer[pos] != 'f' || buffer[pos+1] != 'a' || buffer[pos+2] != 'l' || buffer[pos+3] != 's') throwInvalid("Not expected FALSE");
		ensure(5);
		if (limit - pos < 5  || buffer[pos+4] != 'e') throwInvalid("Not expected FALSE.1");
		pos += 5;
		return false;
	}

	final Boolean parseTrue() throws IOException {
		if(limit - pos < 4) ensure(4);
		if (limit - pos < 4 || (int) JSONStringAddOpens.INT_VIEW.get(buffer, pos) != TRUE_MAGIC) throwInvalid("Not expected TRUE");
		pos += 4;
		return Boolean.TRUE;
	}

	final Boolean parseFalse() throws IOException {
		if(limit - pos < 5) ensure(5);
		// Fast-Path: Prüfe 4 Bytes "fals" und das 5. Byte 'e'
		if ((limit - pos < 5) || (int) JSONStringAddOpens.INT_VIEW.get(buffer, pos) != FALSE_MAGIC || buffer[pos+4] != 'e')
			throwInvalid("Not valid FALSE");
		pos += 5;
		return Boolean.FALSE;
	}


	// ######################################################################################
	// ######################################################################################
	// ######################################################################################

	private int parseHex4() throws IOException {
		if (limit - pos < 4) ensure(4);
		if (limit - pos < 4) throwInvalid("Unexpected end in HEX Escape");
		// Lade alle 4 ASCII-Hex-Zeichen in ein 32-Bit Register
		final var word = (int) JSONStringAddOpens.INT_VIEW.get(buffer, pos);
		pos += 4;
		// --- 1. BRANCHLESS SWAR VALIDATION ---
		final var word_uc = word & 0xDFDFDFDF; // Alle Buchstaben (a-f) zu Uppercase (A-F) zwingen
		// Maske 1: Setzt das 0x80-Bit für alle Bytes, die NICHT zwischen '0' und '9' liegen
		final var not_num   = (word_uc + 0x46464646) | (word_uc - 0x30303030);
		// Maske 2: Setzt das 0x80-Bit für alle Bytes, die NICHT zwischen 'A' und 'F' liegen
		final var not_alpha = (word_uc + 0x39393939) | (word_uc - 0x41414141);
		// Wenn ein Byte WEDER Zahl NOCH Buchstabe ist, hat es in BEIDEN Masken das 0x80 Bit!
		if ((not_num & not_alpha & 0x80808080) != 0) throwInvalid("Invalid HEX escape sequence");
		// --- 2. SWAR DECODING ---
		// Da word_uc jetzt garantiert sauberes Hex ist, dekodieren wir:
		final var mask = word_uc & 0x40404040; // 0x40 für A-F, 0x00 für 0-9
		var val = word_uc - 0x30303030;        // '0'-'9' wird zu 0-9, 'A'-'F' wird zu 17-22
		val -= ((mask >>> 6) * 7);             // Bei 'A'-'F' ziehen wir noch 7 ab -> 10-15
		// Little Endian Byte-Swap
		val = Integer.reverseBytes(val);
		// Komprimierung: Schiebt die Nibbles an die korrekten Positionen im 16-Bit Short
		return ((val >>> 12) & 0xF000) | ((val >>> 8) & 0x0F00) | ((val >>> 4) & 0x00F0) | (val & 0x000F);
	}

	void parseU(int dstLen) throws IOException {
		final var buf = this.buffer;
		var isAscii = true   ;
		var cp = parseHex4();
		if (cp >= 0xD800 && cp <= 0xDBFF) {
			if (limit - pos < 6) ensure(6);
			if (limit - pos >= 6 && buf[pos] == '\\' && buf[pos + 1] == 'u') {
				pos += 2;
				cp = Character.toCodePoint((char) cp, (char) parseHex4());
			}
		}
		if (dstLen + 4 > strBuf.length) expandStrBuf(dstLen + 4);
		if(cp < 0x80) strBuf[dstLen++] = (byte) cp;
		else {
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

	static int computeHash(final byte[] buf, final int off, final int len) {
		var hash64 = HASH_BASIS;
		var pos = off;
		final var limit = off + len;
		final var safeLimit = limit - 8;
		// --- Fast-Path mit VarHandle ---
		while (pos <= safeLimit) {
			final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
			hash64 ^= word;
			hash64 *= HASH_PRIME;
			pos += 8;
		}

		// --- Slow-Path: Tail (Restliche Bytes) ---
		final var remaining = limit - pos;
		if (remaining > 0) {
			var tailWord = 0L;
			// WICHTIG: Hier müssen wir byteweise lesen!
			// Ein LONG_VIEW.get() würde hier eine IndexOutOfBoundsException werfen,
			// wenn wir uns ganz am Ende des Arrays befinden und keine 8 Bytes mehr übrig sind.
			for (var i = 0; i < remaining; i++) {
				tailWord |= (buf[pos + i] & 0xFFL) << (i * 8);
			}

			hash64 ^= tailWord;
			hash64 *= HASH_PRIME;
		}

		// 64-Bit Hash auf 32-Bit Integer falten
		return (int) (hash64 ^ (hash64 >>> 32));
	}

	// SWAR Optimized
	private String parseStringSlow(final int start, final int lPos, boolean isAscii) throws IOException {
		var parsedLen = lPos - start;
		if (parsedLen > strBuf.length) expandStrBuf(parsedLen);
		if (parsedLen > 0) System.arraycopy(buffer, start, strBuf, 0, parsedLen);
		this.pos = lPos;

		Outer: while (true) {
			final var safeLimit = limit - 8;
			var isEscape = false;
			var escChar  = -1; // -1 bedeutet: Muss aus dem RAM geladen werden

			// --- 1. SWAR Fast-Copy Loop ---
			var word     = 0L;
			var has      = 0L;
			var hasQuote = 0L;

			while (pos <= safeLimit) {
				word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				if ((word & JSONString.NON_ASCII_PATTERN) != 0) isAscii = false;
				final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
				final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
				hasQuote               = (quoteXor     - JSONString.SWARN) & ~quoteXor;
				final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
				has                    = (hasQuote | hasBackslash) & JSONString.NON_ASCII_PATTERN;
				if (has != 0) break; // Treffer! Variablen bleiben für Auswertung erhalten
				if (parsedLen + 8 > strBuf.length) expandStrBuf(parsedLen + 8);
				JSONString.LONG_VIEW.set(strBuf, parsedLen, word);
				parsedLen += 8;
				pos       += 8;
			}

			if (has != 0) {
				final var match  = Long.lowestOneBit(has);
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
				if (parsedLen + offset > strBuf.length) expandStrBuf(parsedLen + offset);
				JSONString.LONG_VIEW.set(strBuf, parsedLen, word);
				parsedLen += offset;
				pos       += offset;
				if ((hasQuote & match) != 0) {
					pos++; // Schließendes Quote überspringen
					break Outer; // String komplett zu Ende!
				}
				// Es MUSS ein Backslash sein
				pos++; // Backslash überspringen
				isEscape = true;
				// --- DEIN REGISTER-LOOKAHEAD ---
				if (offset < 7) {
					// Das Escape-Zeichen ist noch im Register!
					// Extrahieren via Shift (0 CPU-Latenz, 0 RAM-Zugriffe)
					escChar = (int) ((word >>> ((offset + 1) << 3)) & 0xFFL);
					pos++; // Wir haben das Zeichen quasi "gelesen"
				}
			}

			// --- 2. SCALAR FALLBACK ---
			if (!isEscape) {
				while (pos < limit) {
					final var b = buffer[pos];
					if (b < 0) isAscii = false;
					if (b == '"') { pos++; break Outer; }
					if (b == '\\') { pos++; isEscape = true; break; }
					if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
					strBuf[parsedLen++] = b;
					pos++;
				}
			}

			// --- 3. REFILL ODER ESCAPE HANDLING ---
			if (!isEscape) {
				// Am Ende des Puffers, ohne Escape -> Nachladen
				ensure(1);
				if (pos >= limit) throwInvalid("Unexpected end of stream inside string");
				continue Outer;
			}

			// Escape auflösen
			var esc = escChar;
			if (esc == -1) {
				// Boundary-Case: Der Backslash war exakt das 8. Byte im Wort.
				// Das nächste Zeichen muss klassisch aus dem Buffer geladen werden.
				if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid("Unexpected end of stream after escape"); }
				esc = buffer[pos++];
			}

			switch (esc) {
			case '"', '\\', '/' -> { }
			case 'b' -> esc = '\b';
			case 'f' -> esc = '\f';
			case 'n' -> esc = '\n';
			case 'r' -> esc = '\r';
			case 't' -> esc = '\t';
			case 'u' -> {
				parseU(parsedLen);
				parsedLen  = escapedParsedLen;
				isAscii   &= escapedIsAscii;
				continue Outer; // parseU hat den Buffer selbst geschrieben
			}
			default -> throwInvalid("Invalid escape sequence");
			}

			if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
			strBuf[parsedLen++] = (byte) esc;
			if (esc < 0) isAscii = false;
		}

		// Konsistenter FNV-1a Hash für den Pool!
		return internBytes(strBuf, 0, parsedLen, computeHash(strBuf, 0, parsedLen), isAscii);
	}

	private int parseStringKeyAsIndexSlow(final Object context, final ObjectMeta meta, final int start, long hash64) throws IOException {
		final var buf = buffer;
		while (pos < limit) {
			final var b = buf[pos];
			if (b == '"') {
				final var len = pos - start;
				pos++;
				// Fold the 64-bit hash state down to a 32-bit integer for the lookup tables
				final var finalHash = (int) (hash64 ^ (hash64 >>> 32));
				final var idx = meta.prepareKey(finalHash, buf, start, len);
				if (idx != -1) return idx;
				final var key = internBytes(buf, start, len, finalHash, true);
				return meta.prepareKey(context, key);
			}

			if (b == '\\' || b < 0) break;
			// Continue the FNV-1a 64-bit hashing sequentially
			hash64 ^= (b & 0xFFL);
			hash64 *= HASH_PRIME;
			pos++;
		}

		// if escapes are present or buffer needs a refill.
		// parseStringSlow computes its own fallback hash, which is fine for this rare edge case.
		final var key = parseStringSlow(start, pos, true);
		return meta.prepareKey(context, key);
	}

	final int parseStringKeyAsIndex(final Object context, final ObjectMeta meta) throws IOException {
		pos++; // Skip the opening quote
		final var start = pos;
		final var safeLimit = limit - 8;
		final var buf = buffer;
		// 64-Bit Hash-Akkumulator
		var hash64 = HASH_BASIS;
		// --- SWAR Fast-Path mit 64-Bit Block-Hashing ---
		while (pos <= safeLimit) {
			final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
			if ((word & JSONString.NON_ASCII_PATTERN) != 0) break; // UTF-8 Slow-Path
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasQuote     = (quoteXor     - 0x0101010101010101L) & ~quoteXor;
			final var hasBackslash = (backslashXor - 0x0101010101010101L) & ~backslashXor;
			final var has = (hasQuote | hasBackslash) & JSONString.NON_ASCII_PATTERN;
			if (has != 0) {
				// Wir haben ein Quote oder Escape gefunden.
				final var match  = Long.lowestOneBit(has); // x86 BLSI Instruktion (1 Zyklus)
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
				final var tailMask = (match >>> 7) - 1L;
				hash64 ^= (word & tailMask);
				hash64 *= HASH_PRIME;
				pos += offset;
				// Wenn das gefundene Bit NICHT in der Quote-Maske ist, war es ein Backslash.
				if ((hasQuote & match) == 0) break; // Backslash gefunden, ab in den Slow-Path
				final var len = pos - start;
				pos++; // Das schließende '"' überspringen
				// Falte den 64-Bit Hash elegant zu einem 32-Bit Integer (für Tabellen/Maps)
				final var finalHash = (int) (hash64 ^ (hash64 >>> 32));
				final var idx = meta.prepareKey(finalHash, buf, start, len);
				if (idx != -1) return idx;
				final var key = internBytes(buf, start, len, finalHash, true);
				return meta.prepareKey(context, key);
			}
			// Voller 8-Byte Block ohne Sonderzeichen!
			// Hash in 2 Taktzyklen: XOR und Multiply. Keine Schleife!
			hash64 ^= word;
			hash64 *= HASH_PRIME;
			pos += 8;
		}
		// Den errechneten 64-Bit Hash als 32-Bit Integer an den Slow-Path übergeben
		final var passedHash = (int) (hash64 ^ (hash64 >>> 32));
		return parseStringKeyAsIndexSlow(context, meta, start, passedHash);
	}

	// SWAR Optimized
	final String parseStringValue() throws IOException {
		pos++; // Überspringe das einleitende '"'
		final var start     = pos;
		final var safeLimit = limit - 8;
		final var buf = buffer;
		while (pos <= safeLimit) { // SWAR fast scan
			// Lade 8 Bytes in einem Rutsch
			final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
			if ((word & JSONString.NON_ASCII_PATTERN) != 0) break; // If byte witch bit 7 set, switch to slow scanning
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor;
			final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
			final var has = (hasQuote | hasBackslash) & JSONString.NON_ASCII_PATTERN;
			if (has != 0) {
				// Ein Treffer liegt vor! Finde heraus, welches Zeichen zuerst kam.
				final var match = Long.lowestOneBit(has);
				// Ermittle den exakten Byte-Offset (0 bis 7)
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
				pos += offset;
				if ((hasQuote & match) != 0) {
					final var len = pos - start;
					pos++; // Das schließende '"' überspringen
					// Only ISO-8859-1 is used avoid JDK UTF-8 conversion.
					return newString(buffer, start, len, true);
				}
				break; // Ein Backslash wurde gefunden, ab in den Slow-Path
			}
			pos += 8;	// No " or \ found, 8 bytes can be used
		}
		// --- SLOW-PATH FALLBACK ---
		// Greift, wenn wir nahe ans Puffer-Ende kommen, UTF-8 auftaucht oder Escapes aufgelöst werden müssen.
		return parseStringSlow(start, pos, true);
	}

	// ######################################################################################
	// ######################################################################################
	// ######################################################################################

	// Bit 9 (\t), 10 (\n), 13 (\r), 32 (Space)
	static final long WHITESPACE_MASK = (1L << 9) | (1L << 10) | (1L << 13) | (1L << 32);

	void skipWhitespace() throws IOException {
		var p = pos;
		final var l = limit;
		final var buf = buffer;
		final var safeLimit = l - 8;
		// --- 1. SWAR FAST-PATH (Der > 32 Scanner) ---
		while (p <= safeLimit) {
			final var word = (long) JSONString.LONG_VIEW.get(buf, p);
			// Magie: Finde das erste Byte > 32 (oder < 0 für UTF-8).
			// 1. (word & 0x7F...) verhindert Überlauf in das nächste Byte.
			// 2. + 0x5F... zwingt jedes Byte >= 33 dazu, das 0x80 Bit zu setzen.
			// 3. | word fängt negative UTF-8 Bytes ab (die haben 0x80 ohnehin gesetzt).
			final var hasPayload = (((word & 0x7F7F7F7F7F7F7F7FL) + 0x5F5F5F5F5F5F5F5FL) | word) & 0x8080808080808080L;

			if (hasPayload != 0) {
				// Wir haben ein strukturelles Zeichen (> 32) gefunden!
				final var offset = (Long.numberOfTrailingZeros(hasPayload) >>> 3);
				pos = p + offset;
				return;
			}
			p += 8; // Komplett übersprungen! Egal ob \n, \t oder Space gemischt waren!
		}

		// --- 2. SCALAR FALLBACK (Am Ende des Buffers) ---
		while (p < l) {
			final var b = buf[p];
			if (b < 0 || ((1L << b) & WHITESPACE_MASK) == 0) {
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
			if ((b & 0xC0) != 0 || ((1L << b) & WHITESPACE_MASK) == 0) return;
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

	void expect(final byte b) throws IOException { final var r = read(); if (r != b) throwInvalid("expect("+(char)b+")!="+(char)r); }

	byte peek() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos];
	}

	byte read() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos++];
	}
}