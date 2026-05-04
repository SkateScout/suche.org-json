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
		final var safeLimitD = dstLen - 16; // 16 Bytes Sicherheit für 8-Byte Accumulator-Writes

		while (sOff < sLen && dstOff < safeLimitD) {

			// --- 1. FAST PATH (Skalares Loop-Unrolling durch C2) ---
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

			while (sOff < sLen && dstOff < safeLimitD) {
				// Flush, wenn der Accumulator Platz für UTF-16 (bis zu 4 Bytes) braucht
				if (accLen > 4) {
					JSONString.LONG_VIEW.set(dst, dstOff, acc);
					dstOff += accLen;
					acc = 0;
					accLen = 0;
				}

				if (c < 256) {
					// Latin1 & Escapes via Lookup-Table
					final var entry = LATIN1_TABLE[c];
					final var len = (int) (entry >>> 56);
					acc |= (entry & 0x00FFFFFFFFFFFFFFL) << (accLen << 3);
					accLen += len;
					sOff++;
				} else // UTF-16 Bit-Packing
					if (Character.isHighSurrogate(c) && sOff + 1 < sLen) {
						final var low = s.charAt(sOff + 1);
						if (Character.isLowSurrogate(low)) {
							final var cp = Character.toCodePoint(c, low);
							final var b1 = (byte) (0xF0 | (cp >> 18)) & 0xFFL;
							final var b2 = (byte) (0x80 | ((cp >> 12) & 0x3F)) & 0xFFL;
							final var b3 = (byte) (0x80 | ((cp >> 6) & 0x3F)) & 0xFFL;
							final var b4 = (byte) (0x80 | (cp & 0x3F)) & 0xFFL;
							acc |= (b1 | (b2 << 8) | (b3 << 16) | (b4 << 24)) << (accLen << 3);
							accLen += 4;
							sOff += 2;
						} else {
							acc |= ((long) '?') << (accLen << 3);
							accLen++;
							sOff++;
						}
					} else {
						if (c >= 0x800) {
							final var b1 = (byte) (0xE0 | (c >> 12)) & 0xFFL;
							final var b2 = (byte) (0x80 | ((c >> 6) & 0x3F)) & 0xFFL;
							final var b3 = (byte) (0x80 | (c & 0x3F)) & 0xFFL;
							acc |= (b1 | (b2 << 8) | (b3 << 16)) << (accLen << 3);
							accLen += 3;
						} else {
							final var b1 = (byte) (0xC0 | (c >> 6)) & 0xFFL;
							final var b2 = (byte) (0x80 | (c & 0x3F)) & 0xFFL;
							acc |= (b1 | (b2 << 8)) << (accLen << 3);
							accLen += 2;
						}
						sOff++;
					}

				if (sOff >= sLen) break;

				// Lade nächstes Zeichen
				c = s.charAt(sOff);

				// Zurück in den Fast-Path, wenn wieder sauberes ASCII vorliegt

				if(c>=32 && c<128 && c != '"' && c != '\\' ) {
					// if (c < 128 && ESCAPE_TABLE[c] == 0) {
					if (accLen > 0) {
						JSONString.LONG_VIEW.set(dst, dstOff, acc);
						dstOff += accLen;
					}
					break;
				}
			}

			// Abschließender Flush des Accumulators, falls wir das Ende des Strings erreicht haben
			// oder den Puffer vorzeitig flushen müssen.
			if (accLen > 0) {
				JSONString.LONG_VIEW.set(dst, dstOff, acc);
				dstOff += accLen;
			}
		}
		// No Tail-Loop recalled after flush from OutputStream
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}
}