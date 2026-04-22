package org.suche.json;

import java.util.Arrays;

final class JSONString {
	private static final byte[] hex = "0123456789abcdef".getBytes();

	// Lookup table for ASCII characters (0-127).
	// 0  = Safe ASCII character (can be copied directly)
	// -1 = Unicode control character requiring \ u00XX escaping
	// >0 = Direct escape character (e.g., 'n' for \n)
	private static final byte[] ESCAPE_TABLE = new byte[128];

	static {
		Arrays.fill(ESCAPE_TABLE, (byte) -1);
		for (var i = 32; i < 128; i++) ESCAPE_TABLE[i] = 0;
		ESCAPE_TABLE['"']  = '"';
		ESCAPE_TABLE['\\'] = '\\';
		ESCAPE_TABLE['\b'] = 'b';
		ESCAPE_TABLE['\f'] = 'f';
		ESCAPE_TABLE['\n'] = 'n';
		ESCAPE_TABLE['\r'] = 'r';
		ESCAPE_TABLE['\t'] = 't';
	}

	public static long encodeChunk(final String s, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS    = sOff;
		final var startD    = dstOff;
		final var safeLimit = dstLen - 6;

		while (sOff < sLen && dstOff < safeLimit) {
			var c = s.charAt(sOff);

			// Fast-path: The C2 JIT compiler can aggressively unroll this loop
			// because the condition is reduced to a single array lookup without branching.
			while (c < 128 && ESCAPE_TABLE[c] == 0) {
				dst[dstOff++] = (byte) c;
				if (++sOff >= sLen || dstOff >= safeLimit) break;
				c = s.charAt(sOff);
			}

			if (sOff >= sLen || dstOff >= safeLimit) break;

			if (c < 128) {
				final var esc = ESCAPE_TABLE[c];
				if (esc > 0) {
					dst[dstOff++] = '\\';
					dst[dstOff++] = esc;
				} else {
					dst[dstOff++] = '\\'; dst[dstOff++] = 'u';
					dst[dstOff++] = '0';  dst[dstOff++] = '0';
					dst[dstOff++] = hex[(c >> 4) & 15]; dst[dstOff++] = hex[c & 15];
				}
				sOff++;
			} else if (Character.isHighSurrogate(c) && sOff + 1 < sLen) {
				final var low = s.charAt(sOff + 1);
				if (Character.isLowSurrogate(low)) {
					final var cp = Character.toCodePoint(c, low);
					dst[dstOff++] = (byte) (0xF0 | (cp >> 18));
					dst[dstOff++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
					dst[dstOff++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
					dst[dstOff++] = (byte) (0x80 | (cp & 0x3F));
					sOff += 2;
				} else {
					dst[dstOff++] = '?';
					sOff++;
				}
			} else {
				if (c >= 0x800) {
					dst[dstOff++] = (byte) (0xE0 | (c >> 12));
					dst[dstOff++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				} else {
					dst[dstOff++] = (byte) (0xC0 | (c >> 6));
				}
				dst[dstOff++] = (byte) (0x80 | (c & 0x3F));
				sOff++;
			}
		}
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}

	static byte[] buildJsonKey(final String name, final boolean keyWrap) {
		final var temp = new byte[name.length() * 6 + 4];
		var pos = 0;
		if (keyWrap) temp[pos++] = '"';
		final var res = encodeChunk(name, 0, name.length(), temp, pos, temp.length);
		pos += (int) res;
		if (keyWrap) {
			temp[pos++] = '"';
			temp[pos++] = ':';
		}
		return java.util.Arrays.copyOf(temp, pos);
	}
}