package org.suche.json;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.suche.json.JSONString.JSONStringProvider;

final class JSONStringAddOpens implements JSONStringProvider {
	private static final VarHandle VALUE_HANDLE;
	private static final VarHandle CODER_HANDLE;
	// Coder constants from java.lang.String
	private static final byte       LATIN1 = 0;
	private static final byte       UTF16  = 1;

	private static final long       UTF16_QUOTE_PATTERN     = 0x0022002200220022L; // '"' in every byte
	private static final long       UTF16_BACKSLASH_PATTERN = 0x005C005C005C005CL; // '\' in every byte
	private static final long       UTF16_NON_ASCII_PATTERN = 0xFF80FF80FF80FF80L; // Sign-Bit in every byte
	private static final long       UTF16_SWARN             = 0x0001000100010001L;

	private static final long       UTF16_CONTROL           = 0x0060006000600060L;
	private static final long       UTF16_CONTROL_MASK      = 0x0080008000800080L;


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

	static final long[] LATIN1_TABLE = new long[256];
	static {
		for (var c = 0; c < 256; c++) {
			byte[] bytes;
			if (c < 32 || c == '"' || c == '\\') {
				// Escape-Logik (\n, \t, \ u00xx)
				bytes = switch (c) {
				case '"'  -> new byte[]{'\\', '"'};
				case '\\' -> new byte[]{'\\', '\\'};
				case '\b' -> new byte[]{'\\', 'b'};
				case '\f' -> new byte[]{'\\', 'f'};
				case '\n' -> new byte[]{'\\', 'n'};
				case '\r' -> new byte[]{'\\', 'r'};
				case '\t' -> new byte[]{'\\', 't'};
				default   -> new byte[]{ '\\', 'u', '0', '0', JSONString.hex[(c >> 4) & 15], JSONString.hex[c & 15] };
				};
			} else if (c < 128) bytes = new byte[]{(byte) c}; // Pures ASCII
			else                bytes = new byte[]{(byte)(0xC0 | (c >> 6)), (byte)(0x80 | (c & 0x3F))}; // 2-Byte UTF-8 für Latin1 > 127
			var word = 0L;
			// Bytes Little-Endian in den long packen
			for (var i = 0; i < bytes.length; i++) word |= ((long) (bytes[i] & 0xFF)) << (8 * i);
			// Länge in das HÖCHSTE Byte packen!
			word |= ((long) bytes.length) << 56;
			LATIN1_TABLE[c] = word;
		}
	}


	@Override public long encodeChunk(final String s, final int sOff, final int sLen, final byte[] dst, final int dstOff, final int dstLen) {
		final var coder = (byte) CODER_HANDLE.get(s);
		if (coder == LATIN1) {
			final var value = (byte[]) VALUE_HANDLE.get(s);
			return encodeLatin1(value, sOff, sLen, dst, dstOff, dstLen);
		}
		if (coder == UTF16 ) {
			final var value = (byte[]) VALUE_HANDLE.get(s);
			return             encodeUTF16(value, sOff, sLen, dst, dstOff, dstLen);
		}

		// Fallback to a UTF16-aware logic or the vanilla logic
		return JSONStringVanilla.INSTANCE.encodeChunk(s, sOff, sLen, dst, dstOff, dstLen);
	}

	static long encodeLatin1(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitS = sLen - 8;
		final var safeLimitD = dstLen - 16;

		// Fast-Path: Bulk copy safe ASCII segments using SWAR directly on the internal array
		while (sOff <= safeLimitS && dstOff <= safeLimitD) {
			final var word = (long) JSONString.LONG_VIEW.get(src, sOff);
			// Speculative write: Overwrite 8 bytes safely
			JSONString.LONG_VIEW.set(dst, dstOff, word);

			// Check for Quote (0x22), Backslash (0x5C), Non-ASCII (>0x7F) or Control (<0x20)
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor     & JSONString.SWARN_MASK_HIGH;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor & JSONString.SWARN_MASK_HIGH;
			final var nonAscii     = word & JSONString.NON_ASCII_PATTERN;
			final var hasControl   = ~((word + JSONString.CONTROL_PATTERN) | word) & JSONString.SWARN_MASK_HIGH;

			final var badMask      = hasQuote | hasBackslash | nonAscii | hasControl;

			if (badMask != 0) {
				// Extract the number of valid bytes before the first special character
				final var safeBytes = (Long.numberOfTrailingZeros(badMask) >>> 3);
				sOff   += safeBytes;
				dstOff += safeBytes;

				// Inline-Verarbeitung des Sonderzeichens
				final var entry = LATIN1_TABLE[src[sOff++] & 0xFF]; // Array-Lookup (O(1)) und sOff Inkrement
				JSONString.LONG_VIEW.set(dst, dstOff, entry);       // Schreibe 8 Bytes (Nutzdaten + Garbage + Längen-Byte)
				dstOff += (int) (entry >>> 56);                     // Verschiebe Offset um tatsächliche Länge
			} else {
				// Everything is safe ASCII, all 8 bytes are used
				sOff   += 8;
				dstOff += 8;
			}
		}

		// --- TAIL CLEANUP LOOP ---
		// Verarbeitet den Rest des Strings (< 8 Bytes) und schützt vor Endlosschleifen
		while (sOff < sLen && dstOff <= dstLen - 8) {
			final var entry = LATIN1_TABLE[src[sOff++] & 0xFF];
			JSONString.LONG_VIEW.set(dst, dstOff, entry);
			dstOff += (int) (entry >>> 56);
		}

		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}

	static long encodeUTF16(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitD = dstLen - 6;

		while (sOff < sLen && dstOff < safeLimitD) {
			// Calculate the maximum number of CHARACTERS we can safely process.
			// We need 4 characters (8 Bytes) in the source array and 4 bytes space in the destination.
			final var remainingS = sLen - sOff;
			final var remainingD = safeLimitD - dstOff;

			// The runLimit ensures that we do not need further bounds checks
			// for sOff or dstOff within the SWAR loop.
			final var runLimit = sOff + Math.min(remainingS & ~3, remainingD & ~3);

			// --- SWAR FAST-PATH (4 Characters per iteration) ---
			while (sOff < runLimit) {
				// Access internal byte[] via VarHandle (sOff << 1 due to UTF-16)
				final var word = (long) JSONString.LONG_VIEW.get(src, sOff << 1);

				// SWAR check for Quote, Backslash or Non-ASCII/High-Byte
				final var quoteXor     = word ^ UTF16_QUOTE_PATTERN;
				final var hasQuote     = (quoteXor - UTF16_SWARN) & ~quoteXor & UTF16_NON_ASCII_PATTERN;
				final var backslashXor = word ^ UTF16_BACKSLASH_PATTERN;
				final var hasBackslash = (backslashXor - UTF16_SWARN) & ~backslashXor & UTF16_NON_ASCII_PATTERN;
				final var nonAscii     = word & UTF16_NON_ASCII_PATTERN;
				final var hasControl = ~((word + UTF16_CONTROL) | word) & UTF16_CONTROL_MASK;
				final var badMask = hasQuote | hasBackslash | nonAscii | hasControl;

				if (badMask != 0) {
					// Calculate the number of safe characters before the first special character
					final var safeChars = (Long.numberOfTrailingZeros(badMask) >>> 4);

					// Write only the safe characters before breaking
					if (safeChars > 0) dst[dstOff]     = (byte) word;
					if (safeChars > 1) dst[dstOff + 1] = (byte) (word >> 16);
					if (safeChars > 2) dst[dstOff + 2] = (byte) (word >> 32);

					sOff   += safeChars;
					dstOff += safeChars;
					break; // Exit to let the scalar path process the exact special character
				}
				// Compress: merge 4 UTF-16 ASCII characters into 4 bytes
				dst[dstOff]     = (byte)  word;
				dst[dstOff + 1] = (byte) (word >> 16);
				dst[dstOff + 2] = (byte) (word >> 32);
				dst[dstOff + 3] = (byte) (word >> 48);

				sOff   += 4;
				dstOff += 4;
			}

			// 2. Scalar fallback (special characters, Unicode or remainder)
			if (sOff < sLen && dstOff < safeLimitD) {
				// Read character from byte[] (Little-Endian)
				final var bytePos = sOff << 1;
				final var c = (char) ((src[bytePos] & 0xFF) | ((src[bytePos + 1] & 0xFF) << 8));
				sOff++;

				if (c < 128) {
					final var esc = JSONStringVanilla.ESCAPE_TABLE[c];	// After SWARN only escape lands here
					if (esc > 0) {
						dst[dstOff++] = '\\';
						dst[dstOff++] = esc;
					} else {
						// Unicode escaping \ u00XX
						dst[dstOff++] = '\\';
						dst[dstOff++] = 'u';
						dst[dstOff++] = '0';
						dst[dstOff++] = '0';
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