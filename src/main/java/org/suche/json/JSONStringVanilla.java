package org.suche.json;

import org.suche.json.JSONString.JSONStringProvider;

final class JSONStringVanilla implements JSONStringProvider {
	// Lookup table for ASCII characters (0-127).
	// 0  = Safe ASCII character (can be copied directly)
	// -1 = Unicode control character requiring \ u00XX escaping
	// >0 = Direct escape character (e.g., 'n' for \n)
	static final JSONStringVanilla INSTANCE = new JSONStringVanilla();
	static final long[] LATIN1_TABLE = JSONString.LATIN1_TABLE;

	@Override public long encodeChunk(final String s, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS     = sOff;
		final var startD     = dstOff;
		final var safeLimitD = dstLen - 16; // 16 Bytes security for 8-byte accumulator writes
		while (sOff < sLen && dstOff < safeLimitD) {
			// 1. FAST PATH: Idiomatic loop for aggressive C2 auto-vectorization (SuperWord optimization)
			final var runLimit = sOff + Math.min(sLen - sOff, safeLimitD - dstOff);
			/*
			var c0 = -1;
			for (; sOff < runLimit; sOff++) {
				c0 = s.charAt(sOff);
				//if (c < 32 || c >= 128 || c == '"' || c == '\\') break;

				// Fast path: Unsigned wrap-around trick combines < 32 and >= 128 into a single check.
				// Bitwise OR (|) prevents short-circuit branching and aids C2 SIMD vectorization.
				if ((((char) (c0 - 32)) > 95) || (c0 == '"') || (c0 == '\\')) break;

				dst[dstOff++] = (byte) c0;
			}
			 */

			final var unrollLimit = runLimit - 3;
			while (sOff < unrollLimit) {
				final var c0 = s.charAt(sOff    );
				final var c1 = s.charAt(sOff + 1);
				final var c2 = s.charAt(sOff + 2);
				final var c3 = s.charAt(sOff + 3);
				if((c0 | c1 | c2 | c3) >= 128) break;
				if (    (c0 < 32) || (c0 == '"') || (c0 == '\\') ||
						(c1 < 32) || (c1 == '"') || (c1 == '\\') ||
						(c2 < 32) || (c2 == '"') || (c2 == '\\') ||
						(c3 < 32) || (c3 == '"') || (c3 == '\\')) {
					break;
				}
				// dst[dstOff    ] = (byte) c0;
				// dst[dstOff + 1] = (byte) c1;
				// dst[dstOff + 2] = (byte) c2;
				// dst[dstOff + 3] = (byte) c3;
				final var packed = c0 | (c1 << 8) | (c2 << 16) | (c3 << 24);
				// Einen 32-Bit Write ausführen
				JSONStringAddOpens.INT_VIEW.set(dst, dstOff, packed);
				sOff   += 4;
				dstOff += 4;
			}

			char c = 0;
			if (sOff < runLimit) {
				c = s.charAt(sOff);
				if (((((char) (c - 32)) <= 95) && (c != '"') && (c != '\\'))) {
					dst[dstOff++] = (byte) c;
					sOff++;
					if (sOff < runLimit) {
						c = s.charAt(sOff);
						if (((((char) (c - 32)) <= 95) && (c != '"') && (c != '\\'))) {
							dst[dstOff++] = (byte) c;
							sOff++;
							if (sOff < runLimit) {
								c = s.charAt(sOff);
								if (((((char) (c - 32)) <= 95) && (c != '"') && (c != '\\'))) {
									dst[dstOff++] = (byte) c;
									sOff++;
								}
							}
						}
					}
				}
			}
			if (sOff >= sLen || dstOff >= safeLimitD) break;
			// 2. UNIFIED ACCUMULATOR (Latin1 + Escapes + UTF-16)
			var acc = 0L;
			var accLen = 0;
			while (dstOff < safeLimitD) {	// sOff < sLen assured by line 30 and 85
				// Flush when the accumulator needs space for UTF-16 (up to 4 bytes)
				if (accLen > 2) {
					JSONString.LONG_VIEW.set(dst, dstOff, acc);
					dstOff += accLen;
					acc = 0;
					accLen = 0;
				}
				final var d = c >>> 8;
					sOff++;
					if(d < 8) {
						if(d == 0) {
							final var entry = LATIN1_TABLE[c];
							acc |= entry << (accLen << 3);
							accLen += (int) (entry >>> 56);
						} else {
							final var utf8 = (0xC0L | (c >> 6))
									|       ((0x80L | (c & 0x3F)) << 8);
							acc |= utf8 << (accLen << 3);
							accLen += 2;
						}
					} else if ((d & 0xF8) != 0xD8) {
						final var utf8 = (0xE0L |  (c >> 12))
								|       ((0x80L | ((c >> 6) & 0x3F)) << 8)
								|       ((0x80L | (c & 0x3F)) << 16);
						acc |= utf8 << (accLen << 3);
						accLen += 3;
					} else {
						var validSurrogate = false;
						if (((d & 0x4) == 0) && (sOff < sLen)) {
							final var low = s.charAt(sOff);
							if ((low & 0xFC00) == 0xDC00) {
								final var cp = (c << 10) + low - 0x35FDC00;
								final var utf8 = (0xF0L | (cp >> 18))
										|       ((0x80L | ((cp >> 12) & 0x3F)) << 8)
										|       ((0x80L | ((cp >> 6)  & 0x3F)) << 16)
										|       ((0x80L | (cp & 0x3F)) << 24);
								acc |= utf8 << (accLen << 3);
								accLen += 4;
								sOff ++;
								validSurrogate = true;
							}
						}
						if (!validSurrogate) {
							acc |= ((long) '?') << (accLen << 3);
							accLen++;
						}
					}
					if (sOff >= sLen) break;
					// Load next character
					c = s.charAt(sOff);
					// Back to the fast-path if clean ASCII is present again
					// 3. ANTI-PING-PONG GUARD:
					// Only jump back into vectorization if there are enough characters left
					// so that the C2 loop overhead is worth it!
					if (((((char) (c - 32)) <= 95) && (c != '"') && (c != '\\')) && (sLen - sOff > 16)) {
						if (accLen > 0) {
							JSONString.LONG_VIEW.set(dst, dstOff, acc);
							dstOff += accLen;
						}
						break;
					}
			}

			// Final flush of the accumulator in case we have reached the end of the string
			// or need to flush the buffer prematurely.
			if (accLen > 0) {
				JSONString.LONG_VIEW.set(dst, dstOff, acc);
				dstOff += accLen;
			}
		}
		// No Tail-Loop recalled after flush from OutputStream
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}
}