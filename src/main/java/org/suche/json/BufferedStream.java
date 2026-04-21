package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.suche.json.ObjectMeta.ArrayBuilder;

sealed class BufferedStream  implements MetaPool permits JsonInputStream {
	private static final int BUFFER_SIZE = 128*1024;
	private final int hashSalt = java.util.concurrent.ThreadLocalRandom.current().nextInt();
	private static final double[] POW10 = { 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18 };
	Map<Object, Object> internPool;
	private final byte[]         numBuf          = new byte[  64];
	byte[]                       strBuf          = new byte[1024];
	private byte[][]             stringPoolKeys;
	private String[]             stringPoolVals;
	private int                  stringPoolSize;
	final byte[]                 buffer          = new byte[BUFFER_SIZE];
	private final Object[][]     arrayPool       = new Object[32][];
	private int                  arrayPoolSize   = 0;

	private final ArrayBuilder[] builderPool     = new ArrayBuilder[32];
	private int                  builderPoolSize = 0;

	private final CtxArrays[]    ctorPool        = new CtxArrays[32];
	private int                  ctorPoolSize    = 0;

	protected InternalEngine engine;
	InputStream in;
	int         pos               ;
	int         limit             ;
	int         depth             ;
	int         maxCollectionSize ;
	int         maxStringLength   ;
	int         maxDepth          ;

	final void init(final InputStream in, final InternalEngine engine) {
		final var cfg          = engine.config();
		this.engine            = engine;
		this.maxDepth          = cfg.maxDepth();
		this.maxCollectionSize = cfg.maxCollectionSize();
		this.maxStringLength   = cfg.maxStringLength();
		this.maxCollectionSize = cfg.maxCollectionSize();
		this.in                = in;
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

	@Override public CtxArrays takeCtxArrays(final int cnt) {
		if (ctorPoolSize > 0) {
			final var c = ctorPool[--ctorPoolSize];
			if (c.objects().length >= cnt) return c;
		}
		return new CtxArrays(new Object[cnt],new long[cnt]);
	}

	@Override public void returnCtxArrays(final CtxArrays c) {
		if (ctorPoolSize < ctorPool.length) {
			Arrays.fill(c.objects(), null);
			Arrays.fill(c.primitives(), 0L);
			ctorPool[ctorPoolSize++] = c;
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

	@Override public ArrayBuilder takeBuilder() {
		if (builderPoolSize > 0) {
			final var b = builderPool[--builderPoolSize];
			b.cnt = 0;
			b.currentKey = null;
			return b;
		}
		return new ArrayBuilder(takeArray(16));
	}

	@Override public void returnBuilder(final ArrayBuilder b) {
		if (builderPoolSize < builderPool.length) {
			Arrays.fill(b.buf, 0, b.cnt, null);
			builderPool[builderPoolSize++] = b;
		}
		else returnArray(b.buf);
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

	private boolean hasExp        = false;
	private int     intDigitCount = 0;
	private int     fracDigits    = 0;
	private long    numberVal     = 0L;
	private int     numberLen     = 0;
	private boolean isNegative    = false;

	private final void parseNumberCore(final Class<?> targetType) throws IOException {
		hasExp        = false;
		intDigitCount = 0;
		fracDigits    = 0;
		numberVal     = 0L;
		numberLen     = 0;
		isNegative    = false;
		ensure(64);
		final var safeBounds = (limit - pos) >= 64;
		var dot           = -1;
		parseLoop: while (true) {
			if (!safeBounds && pos >= limit) { ensure(1); if (pos >= limit) break parseLoop; }
			final var b = buffer[pos++];
			switch (b) {
			case '-' -> { if (numberLen == 1    ) throw new IllegalStateException(); isNegative = true; numBuf[numberLen++] = b; }
			case '.' -> { if (dot >= 0 || hasExp) throw new IllegalStateException(); dot = numberLen  ; numBuf[numberLen++] = b; }
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
				if (!hasExp) {
					if (dot < 0) {
						if (intDigitCount == 1 && numberVal == 0) throw new IllegalStateException();
						intDigitCount++;
						if (intDigitCount <= 18) numberVal = numberVal * 10L + (b & 15);
					} else // Limit to 18 leading significance of double
						if (intDigitCount + fracDigits < 18) {
							fracDigits++;
							numberVal = numberVal * 10L + (b & 15);
						}
				}
				numBuf[numberLen++] = b;
			}
			case 'e', 'E' -> {
				if (hasExp) throw new IllegalStateException();
				hasExp = true;
				numBuf[numberLen++] = b;
				if (!safeBounds && pos >= limit) ensure(1);
				if (pos < limit) {
					final var next = buffer[pos];
					if (next == '+' || next == '-') numBuf[numberLen++] = buffer[pos++];
				}
			}
			default -> { pos--; break parseLoop; }
			}
			if (numberLen == numBuf.length) throw new IllegalStateException();
		}
		if (intDigitCount == 0) throw new IllegalStateException();
	}

	final Object parseNumber(final Class<?> targetType) throws IOException {
		parseNumberCore(targetType);
		if (targetType == boolean.class || targetType == Boolean.class) return numberVal!=0 ? Boolean.TRUE : Boolean.FALSE;
		final var isGeneric = targetType == Object.class || targetType == Number.class;
		if (!hasExp && intDigitCount <= 18) {
			if (isNegative) numberVal = -numberVal;
			if (fracDigits == 0) {
				final var wantInt = targetType == int.class || targetType == Integer.class || (isGeneric && numberVal >= Integer.MIN_VALUE && numberVal <= Integer.MAX_VALUE);
				if (wantInt) return (int) numberVal;
				final var wantLong = targetType == long.class || targetType == Long.class || isGeneric;
				if (wantLong) return numberVal;
			} else if (fracDigits < POW10.length) {
				final var d = numberVal / POW10[fracDigits];
				if (targetType == float.class || targetType == Float.class) return (float) d;
				return d;
			}
		}
		final var d = Double.parseDouble(new String(numBuf, 0, numberLen, StandardCharsets.ISO_8859_1));
		if (targetType == int  .class || targetType == Integer.class) return (int  ) d;
		if (targetType == long .class || targetType == Long   .class) return (long ) d;
		if (targetType == float.class || targetType == Float  .class) return (float) d;
		return d;
	}

	final long parseLongPrimitive() throws IOException {
		parseNumberCore(long.class);
		if (!hasExp && intDigitCount <= 18) {
			if (isNegative) numberVal = -numberVal;
			if (fracDigits == 0) return numberVal;
			if (fracDigits < POW10.length) return (long)(numberVal / POW10[fracDigits]);
		}
		return (long ) Double.parseDouble(new String(numBuf, 0, numberLen, StandardCharsets.ISO_8859_1));
	}

	final double parseDoublePrimitive() throws IOException {
		parseNumberCore(double.class);
		if (!hasExp && intDigitCount <= 18) {
			if (isNegative) numberVal = -numberVal;
			if (fracDigits == 0) return numberVal;
			if (fracDigits < POW10.length) return numberVal / POW10[fracDigits];
		}
		return Double.parseDouble(new String(numBuf, 0, numberLen, StandardCharsets.ISO_8859_1));
	}

	final Object parseNull() throws IOException {
		ensure(4);
		if (limit - pos < 4 || buffer[pos+1] != 'u' || buffer[pos+2] != 'l' || buffer[pos+3] != 'l') throw new IllegalStateException();
		pos += 4;
		return null;
	}

	final void expandStrBuf(final int minCapacity) {
		if (minCapacity > maxStringLength) throw new IllegalStateException();
		var newCapacity = strBuf.length << 1;
		if (newCapacity - minCapacity < 0) newCapacity = minCapacity;
		if (newCapacity < 0) {
			if (minCapacity < 0) throw new OutOfMemoryError();
			newCapacity = Integer.MAX_VALUE - 8;
		}
		strBuf = Arrays.copyOf(strBuf, newCapacity);
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

	@Override
	public final String internBytes(final byte[] src, final int start, final int len, final int hash, final boolean isAscii) {
		if (stringPoolKeys == null || hash == -1) return new String(src, start, len, isAscii ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
		final var mask = stringPoolKeys.length - 1;
		var idx = hash & mask;
		if (stringPoolKeys[idx] instanceof final byte[] k && k.length == len && Arrays.equals(k, 0, len, src, start, start + len))
			return stringPoolVals[idx];
		while (stringPoolKeys[idx] != null) {
			final var k = stringPoolKeys[idx];
			if (k.length == len && Arrays.equals(k, 0, len, src, start, start + len)) return stringPoolVals[idx];
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
					var h = hashSalt;
					for (final byte element : k) h = 31 * h + element;
					var newIdx = h & mask;
					while (stringPoolKeys[newIdx] != null) newIdx = (newIdx + 1) & mask;
					stringPoolKeys[newIdx] = k;
					stringPoolVals[newIdx] = oldV[i];
				}
			}
		}
		return s;
	}

	int     escapedParsedLen;
	boolean escapedIsAscii  ;

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
		return internBytes(strBuf, 0, parsedLen, -1, isAscii);
	}

	final String parseStringKey() throws IOException {
		var       lPos      = pos;
		final var lLimit    = limit;
		final var start     = lPos;
		final var buf       = buffer;
		var       hash      = hashSalt;
		var       asciiMask = 0;
		while (lPos < lLimit) {
			final var b = buf[lPos];
			if (b == '"') {
				pos = lPos + 1;
				return internBytes(buf, start, lPos - start, hash, asciiMask >= 0);
			}
			if (b == '\\') break;
			asciiMask |= b;
			hash = 31 * hash + b;
			lPos++;
		}
		return parseStringSlow(start, lPos, asciiMask >= 0);
	}

	final String parseStringValue() throws IOException {
		var       lPos      = pos;
		final var lLimit    = limit;
		final var start     = lPos;
		final var buf       = buffer;
		var       asciiMask = 0;
		while (lPos < lLimit) {
			final var b = buf[lPos];
			if (b == '"') {
				pos = lPos + 1;
				return internBytes(buf, start, lPos - start, -1, asciiMask >= 0);
			}
			if (b == '\\') break;
			asciiMask |= b;
			lPos++;
		}
		return parseStringSlow(start, lPos, asciiMask >= 0);
	}

	void skipWhitespace() throws IOException {
		var p = pos;
		final var l = limit;
		final var buf = buffer;
		while (p < l) {
			final var b = buf[p];
			if (b > 0x20) { pos = p; return; }
			if (b != 0x20 && b != 0x09 && b != 0x0A && b != 0x0D) { pos = p; return; }
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
		case '{' -> { read(); for (var depth = 1; depth > 0; ) {
			final var c = read();
			switch (c) {
			case '"': skipString(); break;
			case '{': depth++; break;
			case '}': depth--; break;
			default: break;
			}
		}
		}
		case '[' -> { read(); for (var depth = 1; depth > 0; ) {
			final var c = read();
			switch (c) {
			case '"': skipString(); break;
			case '[': depth++; break;
			case ']': depth--; break;
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
			if (c == '\\') { if (pos >= limit) ensure(1); pos++; }
		}
	}

	void expect(final byte b) throws IOException { final var r = read(); if (r != b) throw new IllegalStateException("expect("+(char)b+")!="+(char)r); }
	byte peek() throws IOException { if (pos >= limit) ensure(1); return buffer[pos]; }
	byte read() throws IOException { if (pos >= limit) ensure(1); return buffer[pos++]; }
}