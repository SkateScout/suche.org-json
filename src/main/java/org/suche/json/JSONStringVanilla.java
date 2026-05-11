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

			// --- 1. FAST PATH (Scalar Loop-Unrolling by C2) ---
			final var runLimit = sOff + Math.min(sLen - sOff, safeLimitD - dstOff);
			var c = s.charAt(sOff);
			while (sOff < runLimit) {
				if(c<32 || c>=128 || c == '"' || c == '\\' ) break;
				dst[dstOff++] = (byte) c;
				sOff++;
				if (sOff < runLimit) c = s.charAt(sOff);
			}

			if (sOff >= sLen || dstOff >= safeLimitD) break;

			// --- 2. UNIFIED ACCUMULATOR (Latin1 + Escapes + UTF-16) ---
			var acc = 0L;
			var accLen = 0;

			while (dstOff < safeLimitD) {	// sOff < sLen assured by line 30 and 85
				// Flush when the accumulator needs space for UTF-16 (up to 4 bytes)
				if (accLen > 4) {
					JSONString.LONG_VIEW.set(dst, dstOff, acc);
					dstOff += accLen;
					acc = 0;
					accLen = 0;
				}

				if (c < 32) throw JsonEngine.illegalStateException("Unescaped Control Character");

				if (c < 256) {
					final var entry = LATIN1_TABLE[c];
					// 1. OPTIMIZATION: Removed the masking & 0x00FFFF...L!
					// The length byte (top byte) is automatically shifted out of the 64-bit register
					// by the shift (<<) when accLen > 0 (Overflow).
					acc |= entry << (accLen << 3);
					accLen += (int) (entry >>> 56);
					sOff++;
				} else if (Character.isHighSurrogate(c) && sOff + 1 < sLen) {
					final var low = s.charAt(sOff + 1);
					if (Character.isLowSurrogate(low)) {
						final var cp = Character.toCodePoint(c, low);
						// 2. OPTIMIZATION: Pure 64-bit math without redundant byte casts
						final var utf8 = (0xF0L | (cp >> 18)) |
								((0x80L | ((cp >> 12) & 0x3F)) << 8) |
								((0x80L | ((cp >> 6)  & 0x3F)) << 16) |
								((0x80L | (cp & 0x3F)) << 24);
						acc |= utf8 << (accLen << 3);
						accLen += 4;
						sOff += 2;
					} else {
						acc |= ((long) '?') << (accLen << 3);
						accLen++;
						sOff++;
					}
				} else {
					if (c >= 0x800) {
						final var utf8 = (0xE0L | (c >> 12)) |
								((0x80L | ((c >> 6) & 0x3F)) << 8) |
								((0x80L | (c & 0x3F)) << 16);
						acc |= utf8 << (accLen << 3);
						accLen += 3;
					} else {
						final var utf8 = (0xC0L | (c >> 6)) |
								((0x80L | (c & 0x3F)) << 8);
						acc |= utf8 << (accLen << 3);
						accLen += 2;
					}
					sOff++;
				}

				if (sOff >= sLen) break;

				// Load next character
				c = s.charAt(sOff);

				// Back to the fast-path if clean ASCII is present again
				if(c >= 32 && c < 128 && c != '"' && c != '\\' ) {
					// 3. ANTI-PING-PONG GUARD:
					// Only jump back into vectorization if there are enough characters left
					// so that the C2 loop overhead is worth it!
					if (sLen - sOff > 16) {
						if (accLen > 0) {
							JSONString.LONG_VIEW.set(dst, dstOff, acc);
							dstOff += accLen;
						}
						break;
					}
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