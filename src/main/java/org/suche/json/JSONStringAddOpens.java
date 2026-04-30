package org.suche.json;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.suche.json.JSONString.JSONStringProvider;

final class JSONStringAddOpens implements JSONStringProvider {
	private static final VarHandle VALUE_HANDLE;
	private static final VarHandle CODER_HANDLE;
	// Coder constants from java.lang.String
	private static final byte LATIN1 = 0;
	private static final byte UTF16  = 1;

	static final long       UTF16_QUOTE_PATTERN     = 0x0022002200220022L; // '"' in jedem Byte
	static final long       UTF16_BACKSLASH_PATTERN = 0x005C005C005C005CL; // '\' in jedem Byte
	static final long       UTF16_NON_ASCII_PATTERN = 0xFF80FF80FF80FF80L; // Sign-Bit in jedem Byte
	static final long       UTF16_SWARN             = 0x0001000100010001L;

	static {
		try {
			final var lookup = MethodHandles.privateLookupIn(String.class, MethodHandles.lookup());
			VALUE_HANDLE = lookup.findVarHandle(String.class, "value", byte[].class);
			CODER_HANDLE = lookup.findVarHandle(String.class, "coder", byte.class);
		} catch (final Exception e) {
			// Trigger fallback in the selector
			throw new RuntimeException("add-opens java.base/java.lang is missing", e);
		}
	}

	@Override public long encodeChunk(final String s, final int sOff, final int sLen, final byte[] dst, final int dstOff, final int dstLen) {
		final var coder = (byte) CODER_HANDLE.get(s);
		if (coder == LATIN1) {
			final var value = (byte[]) VALUE_HANDLE.get(s);
			return JSONString.encodeLatin1(value, sOff, sLen, dst, dstOff, dstLen);
		}
		if (coder == UTF16 ) {
			final var value = (byte[]) VALUE_HANDLE.get(s);
			// TODO make it sence to convert byte[] to short[] here ?
			return             encodeUTF16(value, sOff, sLen, dst, dstOff, dstLen);
		}

		// Fallback to a UTF16-aware logic or the vanilla logic
		return JSONStringVanilla.INSTANCE.encodeChunk(s, sOff, sLen, dst, dstOff, dstLen);
	}

	static long encodeUTF16(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitD = dstLen - 6;

		while (sOff < sLen && dstOff < safeLimitD) {
			// 1. Berechne, wie viele ZEICHEN wir maximal sicher verarbeiten können.
			// Wir brauchen im Quell-Array 4 Zeichen (8 Bytes) und im Ziel 4 Bytes Platz.
			final var remainingS = sLen - sOff;
			final var remainingD = safeLimitD - dstOff;

			// Das runLimit stellt sicher, dass wir innerhalb der SWAR-Schleife
			// keine weiteren Bounds-Checks für sOff oder dstOff benötigen.
			final var runLimit = sOff + Math.min(remainingS & ~3, remainingD & ~3);

			// --- SWAR FAST-PATH (4 Zeichen pro Iteration) ---
			while (sOff < runLimit) {
				// Zugriff auf das interne byte[] via VarHandle (sOff << 1 wegen UTF-16)
				final var word = (long) JSONString.LONG_VIEW.get(src, sOff << 1);

				// SWAR-Check auf Quote, Backslash oder Non-ASCII/High-Byte
				final var quoteXor = word ^ UTF16_QUOTE_PATTERN;
				final var hasQuote = (quoteXor - UTF16_SWARN) & ~quoteXor & UTF16_NON_ASCII_PATTERN;

				final var backslashXor = word ^ UTF16_BACKSLASH_PATTERN;
				final var hasBackslash = (backslashXor - UTF16_SWARN) & ~backslashXor & UTF16_NON_ASCII_PATTERN;

				// Falls ein Sonderzeichen oder Non-ASCII gefunden wurde: Abbruch zum skalaren Pfad
				if (hasQuote != 0 || hasBackslash != 0 || (word & UTF16_NON_ASCII_PATTERN) != 0) {
					break;
				}

				// Verdichten: 4 UTF-16 ASCII Zeichen zu 4 Bytes zusammenführen
				dst[dstOff]     = (byte) word;
				dst[dstOff + 1] = (byte) (word >> 16);
				dst[dstOff + 2] = (byte) (word >> 32);
				dst[dstOff + 3] = (byte) (word >> 48);

				sOff += 4;
				dstOff += 4;
			}

			// 2. Skalarer Fallback (Sonderzeichen, Unicode oder Rest)
			if (sOff < sLen && dstOff < safeLimitD) {
				// Zeichen aus dem byte[] lesen (Little-Endian)
				final var bytePos = sOff << 1;
				final var c = (char) ((src[bytePos] & 0xFF) | ((src[bytePos + 1] & 0xFF) << 8));
				sOff++;

				if (c < 128) {
					final var esc = JSONStringVanilla.ESCAPE_TABLE[c];
					if (esc == 0) {
						dst[dstOff++] = (byte) c;
					} else if (esc > 0) {
						dst[dstOff++] = '\\';
						dst[dstOff++] = esc;
					} else {
						// Unicode escaping \ u00XX
						dst[dstOff++] = '\\'; dst[dstOff++] = 'u';
						dst[dstOff++] = '0';  dst[dstOff++] = '0';
						dst[dstOff++] = JSONString.hex[(c >> 4) & 15];
						dst[dstOff++] = JSONString.hex[c & 15];
					}
				} else if (Character.isHighSurrogate(c) && sOff + 1 < sLen) {
					final var low =  (char) ((src[bytePos + 2] & 0xFF) | ((src[bytePos + 3] & 0xFF) << 8));
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
		}
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}
}
