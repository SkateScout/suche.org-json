package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.suche.annotation.NonNull;

abstract sealed class BufferedStream  implements MetaPool permits JsonInputStream {
	private static final int        BUFFER_SIZE = 128*1024;
	private static final int        POOL_SIZE = 128;
	private static final int        STRING_POOL_SIZE = 4096;
	// Little-Endian 32-Bit Konstanten für "true", "fals" und "null"
	private static final int TRUE_MAGIC  = 0x65757274; // 'e'|'u'|'r'|'t'
	private static final int FALSE_MAGIC = 0x736C6166; // 's'|'l'|'a'|'f'
	private static final int NULL_MAGIC  = 0x6C6C756E; // 'l'|'l'|'u'|'n'
	private static final long PATTERN_BRACE_OPEN    = 0x7B7B7B7B7B7B7B7BL; // '{'
	private static final long PATTERN_BRACKET_OPEN  = 0x5B5B5B5B5B5B5B5BL; // '['

	// FNV-1a 64-bit prime
	private static final long HASH_PRIME = 0x100000001b3L;
	// FNV-1a offset basis
	private static final long HASH_BASIS = 0xcbf29ce484222325L;

	Map<Object, Object> internPool;
	byte[]                       strBuf          = new byte[1024];
	private byte[][]             stringPoolKeys;
	private String[]             stringPoolVals;
	private int[]                stringPoolHashes;
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
	long		readDone = 0;

	final Object[] onceInternal = new Object[32];
	int            onceInternalSize = 0;

	private static final String ESCAPE_END = "Unexpected end of stream after escape";

	final void throwInvalid(final String mesg) { throw new IllegalStateException("OFF: "+(readDone+pos)+" "+mesg); }

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
			if (this.internPool       == null) {
				this.internPool       = new java.util.HashMap<>();
				this.stringPoolKeys   = new byte  [STRING_POOL_SIZE][];
				this.stringPoolVals   = new String[STRING_POOL_SIZE];
				this.stringPoolHashes = new int   [STRING_POOL_SIZE];
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
		ctx.singleType = PRIMITIVE.T_EMPTY;
		ctx.currentKey = null;
		ctx.objs       = null;
		ctx.prims      = null;
		ctx.map        = map ? 1 : 0;
		return ctx;
	}

	@Override public void returnContext(final @NonNull ParseContext ctx) {
		if (ctx.objs != null) {
			returnArray(ctx.objs, ctx.cnt);
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

	@Override public final String internBytes(final byte[] src, final int start, final int len, final int hash, final boolean isAscii) {
		// Strings over 36 Zeichen (Text, long URLs) are not pooled or uuids
		if (stringPoolKeys == null || len > 36 || (len == 36 && src[start + 8] == '-')) return newString(src, start, len, isAscii);
		final var keys = this.stringPoolKeys;
		final var mask = keys.length - 1;
		var idx = hash & mask;
		while (true) {
			final var k = keys[idx];
			if (k == null) break;
			// ZERO-ALLOCATION FAST-MISS: Integer-Vergleich VOR dem teuren Arrays.equals!
			if (stringPoolHashes[idx] == hash && k.length == len && Arrays.equals(k, 0, len, src, start, start + len)) {
				return stringPoolVals[idx];
			}
			idx = (idx + 1) & mask;
		}
		// Hash wird direkt weitergegeben
		return internBytesSlow(src, start, len, isAscii, mask, idx, hash);
	}

	// Perfekte Bit-Verteilung (Avalanche) für den 64-Bit Hash
	static int finalizeHash(long hash64) {
		hash64 ^= hash64 >>> 33;
		hash64 *= 0xff51afd7ed558ccdL;
		hash64 ^= hash64 >>> 33;
		hash64 *= 0xc4ceb9fe1a85ec53L;
		hash64 ^= hash64 >>> 33;
		return (int) hash64;
	}

	private String internBytesSlow(final byte[] src, final int start, final int len, final boolean isAscii, int mask, final int idx, final int hash) {
		// --- DIE NOTBREMSE ---
		if (stringPoolSize >= maxCollectionSize) {
			// Der Pool ist "vergiftet" mit zu vielen einmaligen IDs.
			// Wir flushen ihn, um Platz für wichtige, wiederkehrende Keys zu machen.
			java.util.Arrays.fill(stringPoolKeys, null);
			java.util.Arrays.fill(stringPoolVals, null);
			java.util.Arrays.fill(stringPoolHashes, 0);
			stringPoolSize = 0;
			// Wir berechnen den Index für das GANZ leere Array neu
			mask = stringPoolKeys.length - 1;
		}

		final var s = newString(src, start, len, isAscii);

		// Falls wir gerade geflusht haben, müssen wir den neuen Slot suchen
		var insertIdx = idx;
		if (stringPoolSize == 0) insertIdx = hash & mask;

		stringPoolKeys[insertIdx]   = java.util.Arrays.copyOfRange(src, start, start + len);
		stringPoolVals[insertIdx]   = s;
		stringPoolHashes[insertIdx] = hash;

		if (++stringPoolSize > stringPoolKeys.length * 0.75f) {
			final var oldK = stringPoolKeys;
			final var oldV = stringPoolVals;
			final var oldH = stringPoolHashes;

			stringPoolKeys   = new byte[oldK.length << 1][];
			stringPoolVals   = new String[oldV.length << 1];
			stringPoolHashes = new int [oldH.length << 1];
			mask = stringPoolKeys.length - 1;

			for (var i = 0; i < oldK.length; i++) {
				final var k = oldK[i];
				if (k != null) {
					final var h = oldH[i];
					var newIdx = h & mask;
					while (stringPoolKeys[newIdx] != null) newIdx = (newIdx + 1) & mask;
					stringPoolKeys[newIdx]   = k;
					stringPoolVals[newIdx]   = oldV[i];
					stringPoolHashes[newIdx] = h;
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
		readDone += pos;
		pos = 0;
		limit = remaining;
		if (in != null) {
			// Reserve 8 bytes at the very end of the buffer for the sentinel padding
			final var maxLimit = buffer.length - 8;
			while (limit < n) {
				final var count = in.read(buffer, limit, maxLimit - limit);
				if (count == -1) break;
				limit += count;
			}
		}
		JSONString.LONG_VIEW.set(buffer, limit, 0x2020202020202020L);
	}

	@Override
	public Object deduplicate(final Object value) {
		if (internPool == null || value == null) return value;
		final var existing = internPool.putIfAbsent(value, value);
		return existing != null ? existing : value;
	}

	private final NumberParser numberParser = new NumberParser();

	// ---------------------------------------------------------
	// Public parser methods
	// ---------------------------------------------------------

	final double parseDoublePrimitive() throws IOException {
		ensure(64);
		pos = numberParser.parseNumberCore(buffer, pos);
		return numberParser.computeDoubleValue();
	}

	final long parseLongPrimitive() throws IOException {
		ensure(64);
		pos = numberParser.parseNumberCore(buffer, pos);
		return numberParser.computeLongValue();
	}

	final Object parseNumber(final long typeDesc) throws IOException {
		ensure(64);
		pos = numberParser.parseNumberCore(buffer, pos);

		final var isPrimitive = (typeDesc >= 0L) && ((typeDesc & 1L) != 0L);
		final var metaId = typeDesc >= 0L ? (int)(typeDesc >>> 1) : ObjectMeta.IDX_GENERIC;

		if (isPrimitive && metaId == ObjectMeta.PRIM_BOOLEAN) return numberParser.numberVal() != 0 ? Boolean.TRUE : Boolean.FALSE;

		final var wantFloat = isPrimitive ? (metaId == ObjectMeta.PRIM_DOUBLE || metaId == ObjectMeta.PRIM_FLOAT) : numberParser.isFloat();

		if (wantFloat || numberParser.isFloat()) {
			final var d = numberParser.computeDoubleValue();
			if (isPrimitive && metaId == ObjectMeta.PRIM_FLOAT) return (float) d;
			return d;
		}

		final var v = numberParser.computeLongValue();
		// Auto-Downcast für Generics oder explizite INT Anforderung
		final var wantInt = isPrimitive ? (metaId == ObjectMeta.PRIM_INT) : (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE);

		if (wantInt) return (int) v;
		return v;
	}

	final void parseNumericPrimitive(final ObjectMeta meta, final Object ctx, final int targetIdx, final long typeDesc) throws IOException {
		ensure(64);
		pos = numberParser.parseNumberCore(buffer, pos);

		// Fast-Path mit Bit-Operation (Prüfung auf Primitiv-Flag auf Bit 0)
		if (typeDesc >= 0L && (typeDesc & 1L) != 0L) {
			final var primId = (int) (typeDesc >>> 1);
			if (primId == ObjectMeta.PRIM_DOUBLE || primId == ObjectMeta.PRIM_FLOAT ||
					(numberParser.isFloat() && primId != ObjectMeta.PRIM_LONG && primId != ObjectMeta.PRIM_INT)) {
				meta.setDouble(this, ctx, targetIdx, numberParser.computeDoubleValue());
			} else {
				meta.setLong(this, ctx, targetIdx, numberParser.computeLongValue());
			}
		} else // Generics, Wrapper (Long, Double), Map, Object -> Entscheidung anhand des echten JSON-Werts
			if (numberParser.isFloat()) {
				meta.setDouble(this, ctx, targetIdx, numberParser.computeDoubleValue());
			} else {
				meta.setLong(this, ctx, targetIdx, numberParser.computeLongValue());
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

	private static final String UNESCAPE_CONTRL = "Unescaped control character in string";

	private int parseHex4() throws IOException {
		if (limit - pos < 4) ensure(4);
		if (limit - pos < 4) throwInvalid("Unexpected end in HEX Escape");
		final var word = (int) JSONStringAddOpens.INT_VIEW.get(buffer, pos);
		pos += 4;
		// Vorab-Check: Wenn irgendein Byte > 127 ist, ist es kein gültiges ASCII-Hex.
		if ((word & 0x80808080) != 0) throwInvalid("Invalid HEX escape sequence: " + java.util.HexFormat.of().formatHex(buffer, pos - 4, pos));
		// check '0'-'9' (0x30 bis 0x39)
		final var is_num   = (word + 0x50505050) & ~(word + 0x46464646);
		// check 'A'-'F' (0x41 bis 0x46)
		final var is_upper = (word + 0x3F3F3F3F) & ~(word + 0x39393939);
		// check 'a'-'f' (0x61 bis 0x66)
		final var is_lower = (word + 0x1F1F1F1F) & ~(word + 0x19191919);
		// Jedes der 4 Bytes MUSS in genau einer der drei Gruppen das 0x80-Bit gesetzt haben.
		if (((is_num | is_upper | is_lower) & 0x80808080) != 0x80808080) throwInvalid("Invalid HEX escape sequence: " + java.util.HexFormat.of().formatHex(buffer, pos - 4, pos));
		final var letterMask = word & 0x40404040;
		final var add9 = (letterMask >>> 6) * 9;
		final var val0 = (word & 0x0F0F0F0F) + add9;
		final var val1 = Integer.reverseBytes(val0);
		final var val2 = (val1 | (val1 >>> 4)) & 0x00FF00FF;
		return (val2 | (val2 >>> 8)) & 0xFFFF;
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
		return finalizeHash(hash64);
	}

	// SWAR Optimized
	final String parseStringValue() throws IOException {
		pos++; // Skip the opening '"'
		final var start     = pos;
		final var safeLimit = limit - 8;
		final var buf = buffer;
		while (pos <= safeLimit) { // SWAR fast scan
			// Load 8 bytes at once
			final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
			if ((word & JSONString.NON_ASCII_PATTERN) != 0) break; // If byte with bit 7 set, switch to slow scanning
			final var quoteXor     = word          ^ JSONString.QUOTE_PATTERN;
			final var backslashXor = word          ^ JSONString.BACKSLASH_PATTERN;
			final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor;
			final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
			final var hasCtrl      = (word - 0x2020202020202020L) & ~word;
			final var has          = (hasQuote | hasBackslash | hasCtrl) & JSONString.NON_ASCII_PATTERN;
			if (has != 0) {
				// A match exists! Find out which character came first.
				final var match = Long.lowestOneBit(has);
				if ((hasCtrl & match) != 0) throwInvalid(UNESCAPE_CONTRL);
				// Determine the exact byte offset (0 to 7)
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
				pos += offset;
				if ((hasQuote & match) != 0) {
					final var len = pos - start;
					pos++; // Skip the closing '"'
					// Only ISO-8859-1 is used to avoid JDK UTF-8 conversion.
					return newString(buffer, start, len, true);
				}
				break; // A backslash was found, jump to slow path
			}
			pos += 8;	// No " or \ found, 8 bytes can be used
		}
		// --- SLOW-PATH FALLBACK ---
		// Takes effect when we get close to the buffer end, UTF-8 appears, or escapes need to be resolved.
		return parseStringSlow(start, pos, true, false);
	}

	final int parseStringKeyAsIndex(final Object context, final ObjectMeta meta) throws IOException {
		pos++; // Skip the opening quote
		final var start = pos;
		final var safeLimit = limit - 8;
		final var buf = buffer;
		// 64-bit hash accumulator
		var hash64 = HASH_BASIS;
		// --- SWAR Fast-Path with 64-bit block hashing ---
		while (pos <= safeLimit) {
			final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
			if ((word & JSONString.NON_ASCII_PATTERN) != 0) break; // UTF-8 Slow-Path
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasQuote     = (quoteXor     - 0x0101010101010101L) & ~quoteXor;
			final var hasBackslash = (backslashXor - 0x0101010101010101L) & ~backslashXor;
			final var hasCtrl      = (word - 0x2020202020202020L) & ~word;
			final var has = (hasQuote | hasBackslash | hasCtrl) & JSONString.NON_ASCII_PATTERN;
			if (has != 0) {
				// We found a quote, escape or control character.
				final var match  = Long.lowestOneBit(has); // x86 BLSI instruction (1 cycle)
				if ((hasCtrl & match) != 0) throwInvalid("Unescaped control character in key");
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
				if(match>0x80L) {
					final var tailMask = (match >>> 7) - 1L;
					hash64 ^= (word & tailMask);
					hash64 *= HASH_PRIME;
				}
				pos += offset;
				// If the found bit is NOT in the quote mask, it was a backslash.
				if ((hasQuote & match) == 0) break; // Backslash found, jump to slow path
				final var len = pos - start;
				pos++; // Skip the closing '"'
				// Fold the 64-bit hash elegantly into a 32-bit integer (for tables/maps)
				final var finalHash = finalizeHash(hash64);
				final var idx = meta.prepareKey(finalHash, buf, start, len);
				if (idx != -1) return idx;
				final var key = internBytes(buf, start, len, finalHash, true);
				return meta.prepareKey(context, key);
			}
			// Full 8-byte block without special characters!
			// Hash in 2 clock cycles: XOR and Multiply. No loop!
			hash64 ^= word;
			hash64 *= HASH_PRIME;
			pos += 8;
		}
		// Pass the calculated 64-bit hash as 32-bit integer to the slow path
		final var passedHash = finalizeHash(hash64);
		return parseStringKeyAsIndexSlow(context, meta, start, passedHash);
	}

	private int parseStringKeyAsIndexSlow(final Object context, final ObjectMeta meta, final int start, long hash64) throws IOException {
		final var buf = buffer;
		var tailWord = 0L;
		var tailLen = 0;

		while (pos < limit) {
			final var b = buf[pos];
			if (b >= 0 && b < 32) throwInvalid("Unescaped control character in key");
			if (b == '"') {
				final var len = pos - start;
				pos++;

				// Process any remaining bytes in the accumulator identically to computeHash
				if (tailLen > 0) {
					hash64 ^= tailWord;
					hash64 *= HASH_PRIME;
				}

				final var finalHash = finalizeHash(hash64);
				final var idx = meta.prepareKey(finalHash, buf, start, len);
				if (idx != -1) return idx;
				final var key = internBytes(buf, start, len, finalHash, true);
				return meta.prepareKey(context, key);
			}

			if (b == '\\' || b < 0) break; // Escapes or UTF-8

			// Pack byte into tailWord (Little-Endian)
			tailWord |= (b & 0xFFL) << (tailLen * 8);
			tailLen++;
			pos++;

			// If we accumulate a full 8-byte block, process it
			if (tailLen == 8) {
				hash64 ^= tailWord;
				hash64 *= HASH_PRIME;
				tailWord = 0L;
				tailLen = 0;
			}
		}

		// If we break due to '\\' or UTF-8, the raw bytes no longer represent the final string.
		// parseStringSlow will build the unescaped string and compute the hash from scratch.
		final var key = parseStringSlow(start, pos, true, true);
		return meta.prepareKey(context, key);
	}

	// SWAR Optimized
	private String parseStringSlow(final int start, final int lPos, boolean isAscii, final boolean asKey) throws IOException {
		var parsedLen = lPos - start;
		if (parsedLen > strBuf.length) expandStrBuf(parsedLen);
		if (parsedLen > 0) System.arraycopy(buffer, start, strBuf, 0, parsedLen);
		this.pos = lPos;

		Outer: while (true) {
			final var safeLimit = limit - 8;
			var isEscape = false;
			var escChar  = -1; // -1 means: must be loaded from RAM
			// --- 1. SWAR Fast-Copy Loop ---
			var word     = 0L;
			var has      = 0L;
			var hasQuote = 0L;
			var hasCtrl  = 0L;

			while (pos <= safeLimit) {
				word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				if ((word & JSONString.NON_ASCII_PATTERN) != 0) isAscii = false;
				final var quoteXor     =  word         ^ JSONString.QUOTE_PATTERN    ;
				final var backslashXor =  word         ^ JSONString.BACKSLASH_PATTERN;
				hasQuote               = (quoteXor     - JSONString.SWARN) & ~quoteXor;
				final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
				hasCtrl                = (word - 0x2020202020202020L) & ~word;
				has                    = (hasQuote | hasBackslash | hasCtrl) & JSONString.NON_ASCII_PATTERN;
				if (has != 0) break; // Match! Variables are preserved for evaluation
				if (parsedLen + 8 >= strBuf.length) expandStrBuf(parsedLen + 8);
				JSONString.LONG_VIEW.set(strBuf, parsedLen, word);
				parsedLen += 8;
				pos       += 8;
			}

			if (has != 0) {
				final var match  = Long.lowestOneBit(has);
				if ((hasCtrl & match) != 0) throwInvalid(UNESCAPE_CONTRL);
				final var offset = (Long.numberOfTrailingZeros(match) >>> 3);

				// --- FIX: Immer für 8 Bytes Platz schaffen ---
				// Da LONG_VIEW.set unweigerlich 8 Bytes schreibt, auch wenn wir
				// durch 'offset' nur z. B. 2 Bytes davon als echten String werten.
				if (parsedLen + 8 > strBuf.length) expandStrBuf(parsedLen + 8);

				JSONString.LONG_VIEW.set(strBuf, parsedLen, word);
				parsedLen += offset;
				pos       += offset;
				if ((hasQuote & match) != 0) {
					pos++; // Skip closing quote
					break Outer; // String completely finished!
				}
				// It MUST be a backslash
				pos++; // Skip backslash
				isEscape = true;
				// --- REGISTER-LOOKAHEAD ---
				if (offset < 7) {
					// The escape character is still in the register!
					escChar = (int) ((word >>> ((offset + 1) << 3)) & 0xFFL);
					pos++;
				}
			}

			// --- 2. SCALAR FALLBACK ---
			if (!isEscape) {
				while (pos < limit) {
					final var b = buffer[pos];
					if (b >= 0 && b < 32) throwInvalid(UNESCAPE_CONTRL);
					if (b < 0) isAscii = false;
					if (b == '"') { pos++; break Outer; }
					if (b == '\\') { pos++; isEscape = true; break; }
					if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
					strBuf[parsedLen++] = b;
					pos++;
				}
			}

			// --- 3. REFILL OR ESCAPE HANDLING ---
			if (!isEscape) {
				// At end of buffer, no escape -> Refill
				ensure(1);
				if (pos >= limit) throwInvalid("Unexpected end of stream inside string");
				continue Outer;
			}

			// Resolve escape
			var esc = escChar;
			if (esc == -1) {
				// Boundary-Case: The backslash was exactly the 8th byte in the word.
				// The next character must be loaded classically from the buffer.
				if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid(ESCAPE_END); }
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
				continue Outer; // parseU has written the buffer itself
			}
			default -> throwInvalid("Invalid escape sequence");
			}

			if (parsedLen == strBuf.length) expandStrBuf(parsedLen + 1);
			strBuf[parsedLen++] = (byte) esc;
			if (esc < 0) isAscii = false;
		}
		if (!asKey) return newString(strBuf, 0, parsedLen, isAscii);
		// Consistent FNV-1a Hash for the pool!
		return internBytes(strBuf, 0, parsedLen, computeHash(strBuf, 0, parsedLen), isAscii);
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

	private final void skipComplexValue(final long openPattern) throws IOException {
		ensure(16); // Optional: Kann auch weg, wenn in skipValue schon ensure() gerufen wurde
		var curDepth = 1;
		final var buf = buffer;
		while (true) {
			while (pos < limit) {
				final var word      = (long) JSONString.LONG_VIEW.get(buf, pos);
				final var shifted   = word     - 0x0202020202020202L;
				final var orEd      = shifted  | 0x0202020202020202L;
				final var structX   = orEd     ^ openPattern;
				final var quoteX    = word     ^ JSONString.QUOTE_PATTERN;
				final var hasStruct = (structX - JSONString.SWARN) & ~structX;
				final var hasQuote  = (quoteX  - JSONString.SWARN) & ~quoteX;
				final var has = (hasStruct | hasQuote) & JSONString.NON_ASCII_PATTERN;
				final var bitOffset = Long.numberOfTrailingZeros(has);
				pos += (bitOffset >>> 3);
				if (has != 0) {
					final var c = (byte) (word >>> bitOffset);
					pos++;
					if (c == '"') { skipString(); continue; }
					curDepth += (c & 2) - 1;
				}
				if (curDepth == 0) return;
			}
			pos = limit;
			ensure(16);
			if (limit == 0) throwInvalid("Unexpected end of stream in skipValue");
		}
	}

	final void skipValue() throws IOException {
		ensure(16);
		if(limit==0) throwInvalid("Skio EOF");
		final var b = buffer[pos++];
		switch (b) {
		case '{' -> skipComplexValue(PATTERN_BRACE_OPEN  );
		case '[' -> skipComplexValue(PATTERN_BRACKET_OPEN);
		case '"' -> skipString();
		default  -> skipSkalarValue();
		}
	}

	private final void skipSkalarValue() throws IOException {
		while (true) {
			final var buf = buffer;
			while (pos < limit) {
				final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
				final var braceOrBracket = word           | 0x2020202020202020L;	// make ] to }
				final var commaXor       = word           ^ 0x2C2C2C2C2C2C2C2CL; // ',' (0x2C)
				final var closeXor       = braceOrBracket ^ 0x7D7D7D7D7D7D7D7DL;
				final var hasComma       = (commaXor - JSONString.SWARN) & ~commaXor;
				final var hasClose       = (closeXor - JSONString.SWARN) & ~closeXor;
				final var has = (hasClose | hasComma) & JSONString.NON_ASCII_PATTERN;
				if (has != 0) {
					pos += (Long.numberOfTrailingZeros(has) >>> 3);
					return;
				}
				pos += 8;
			}
			if (pos > limit) pos = limit;
			ensure(16);
			if (pos >= limit) throwInvalid("Unexpected end of stream while skipping value");
		}
	}

	final void skipString() throws IOException {
		ensure(16);
		while (true) {
			final var buf = buffer;
			while (pos < limit) {
				final var word = (long) JSONString.LONG_VIEW.get(buf, pos);
				final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
				final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
				final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor;
				final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
				final var has          = (hasQuote | hasBackslash) & JSONString.NON_ASCII_PATTERN;
				if (has == 0) pos += 8;
				else {
					final var match = Long.lowestOneBit(has);
					final var offset = (Long.numberOfTrailingZeros(match) >>> 3);
					if ((hasQuote & match) != 0) { pos += offset + 1;  return; } // First match was "
					pos += offset + 2;	// Skip Qute and next char
					if (pos > limit) {	// Edge Case \ before limit
						pos = limit;
						ensure(16);
						if (pos >= limit) throwInvalid(ESCAPE_END);
						pos++;
					}
				}
			}
			if (pos > limit) pos = limit;
			ensure(16);
			if (pos >= limit) throwInvalid("Unexpected end of stream inside string");
		}
	}

	final  void unexpect(final byte e, final byte g) { throwInvalid("expect("+(char)e+")!="+(char)g+")"); }

	void expect(final byte b) throws IOException { final var r = read(); if (r != b) unexpect(b, r); }

	byte peek() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos];
	}

	byte read() throws IOException {
		if (pos >= limit) ensure(1);
		return buffer[pos++];
	}
}