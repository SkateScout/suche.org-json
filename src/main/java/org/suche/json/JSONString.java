package org.suche.json;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class JSONString {
	static final byte[]     hex               = "0123456789abcdef".getBytes();
	static final VarHandle  LONG_VIEW         = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
	static final long       QUOTE_PATTERN     = 0x2222222222222222L; // '"' in jedem Byte
	static final long       BACKSLASH_PATTERN = 0x5C5C5C5C5C5C5C5CL; // '\' in jedem Byte
	static final long       NON_ASCII_PATTERN = 0x8080808080808080L; // Sign-Bit in jedem Byte
	static final long       SWARN             = 0x0101010101010101L;

	sealed interface JSONStringProvider permits JSONStringVanilla, JSONStringAddOpens {
		// Returns (sOff << 32) | dstOff
		long encodeChunk(String s, int sOff, int sLen, byte[] dst, int dstOff, int dstLen);
	}

	private static final JSONStringProvider PROVIDER;

	static {
		JSONStringProvider selected;
		try { selected = new JSONStringAddOpens(); } // 2. Test for Add-Opens access to String.value
		catch (final Throwable _) { selected = JSONStringVanilla.INSTANCE; } // 3. Fallback to Vanilla (always works)
		PROVIDER = selected;
	}

	static long encodeChunk(final String s, final int sOff, final int sLen, final byte[] dst, final int dstOff, final int dstLen) {
		return PROVIDER.encodeChunk(s, sOff, sLen, dst, dstOff, dstLen);
	}

	static byte[] buildJsonKey(final String name, final boolean keyWrap) {
		final var temp = new byte[name.length() * 6 + 6];
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

	static long encodeLatin1(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitD = dstLen - 6;

		while (sOff < sLen && dstOff < safeLimitD) {
			// Fast-Path: Bulk copy safe ASCII segments using SWAR directly on the internal array
			final var remainingS = sLen - sOff;
			final var remainingD = safeLimitD - dstOff;
			final var runLimit = sOff + Math.min(remainingS, remainingD);

			// SWAR scan on the INTERNAL string array
			while (sOff <= runLimit - 8) {
				final var word = (long) JSONString.LONG_VIEW.get(src, sOff);

				// Check for Quote (0x22), Backslash (0x5C) or Non-ASCII (>0x7F)
				final var quoteXor = word ^ JSONString.QUOTE_PATTERN;
				final var hasQuote = (quoteXor - SWARN) & ~quoteXor & JSONString.NON_ASCII_PATTERN;

				final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
				final var hasBackslash = (backslashXor - SWARN) & ~backslashXor & JSONString.NON_ASCII_PATTERN;

				if ((hasQuote != 0) || (hasBackslash != 0) || ((word & JSONString.NON_ASCII_PATTERN) != 0)) {
					break;
				}
				// Everything is safe ASCII: Copy 8 bytes at once from array to array
				JSONString.LONG_VIEW.set(dst, dstOff, word);
				sOff += 8;
				dstOff += 8;
			}

			// Standard scalar processing for remaining/special chars
			if (sOff < sLen && dstOff < safeLimitD) {
				final var c = src[sOff++];
				final var esc = JSONStringVanilla.ESCAPE_TABLE[c];
				if (esc == 0)
					dst[dstOff++] = c;
				else if (esc > 0) {
					dst[dstOff++] = '\\';
					dst[dstOff++] = esc;
				} else {
					dst[dstOff++] = '\\'; dst[dstOff++] = 'u';
					dst[dstOff++] = '0';  dst[dstOff++] = '0';
					dst[dstOff++] = JSONString.hex[(c >> 4) & 15]; dst[dstOff++] = JSONString.hex[c & 15];
				}
			}
		}
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}

}