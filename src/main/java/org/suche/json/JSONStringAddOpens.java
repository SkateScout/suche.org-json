package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.suche.json.JSONString.JSONStringProvider;

final class JSONStringAddOpens implements JSONStringProvider {
	static final VarHandle VALUE_HANDLE;
	static final VarHandle CODER_HANDLE;
	// Coder constants from java.lang.String
	static final byte       LATIN1 = 0;
	static final byte       UTF16  = 1;
	static final VarHandle INT_VIEW   = MethodHandles.byteArrayViewVarHandle(int  [].class, ByteOrder.LITTLE_ENDIAN);
	static final VarHandle SHORT_VIEW = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
	static final MethodHandle STRING_SECRET_CONSTRUCTOR;
	private static final long       UTF16_QUOTE_PATTERN     = 0x0022002200220022L; // '"' in every byte
	private static final long       UTF16_BACKSLASH_PATTERN = 0x005C005C005C005CL; // '\' in every byte
	private static final long       UTF16_NON_ASCII_PATTERN = 0xFF80FF80FF80FF80L; // Sign-Bit in every byte
	private static final long       UTF16_SWARN             = 0x0001000100010001L;
	private static final long       UTF16_CONTROL           = 0x0060006000600060L;
	private static final long       UTF16_CONTROL_MASK      = 0x0080008000800080L;
	private static final long 		MAGIC_4X16_to_4X8       = 0x0001000001000001L;

	private JSONStringAddOpens() { }
	static final JSONStringAddOpens ONCE;

	static {
		JSONStringAddOpens o = null;
		VarHandle vh = null;
		VarHandle ch = null;
		MethodHandle mh = null;
		try {
			final var lookup = MethodHandles.privateLookupIn(String.class, MethodHandles.lookup());
			vh = lookup.findVarHandle(String.class, "value", byte[].class);
			ch = lookup.findVarHandle(String.class, "coder", byte.class);
			mh = lookup.findConstructor(String.class, MethodType.methodType(void.class, byte[].class, byte.class));
			o = new JSONStringAddOpens();
		} catch (final Exception _) {
			// Trigger fallback in the selector
		}
		STRING_SECRET_CONSTRUCTOR = mh;
		VALUE_HANDLE = vh;
		CODER_HANDLE = ch;
		ONCE = o;
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
		final var startS     = sOff;
		final var startD     = dstOff;
		final var safeLimitS = sLen - 8;
		final var safeLimitD = dstLen - 16;
		// Fast-Path: Bulk copy safe ASCII segments using SWAR directly on the internal array
		while (sOff <= safeLimitS && dstOff <= safeLimitD) {
			final var word = (long) JSONString.LONG_VIEW.get(src, sOff);
			// Speculative write: Overwrite 8 bytes safely
			JSONString.LONG_VIEW.set(dst, dstOff, word);
			// Check for Quote (0x22), Backslash (0x5C), Non-ASCII (>0x7F) or Control (<0x20)
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
			final var hasControl   = ~((word + JSONString.CONTROL_PATTERN) | word);
			// Combine using boolean distributive property
			final var badMask      = (hasQuote | hasBackslash | word | hasControl) & JSONString.SWARN_MASK_HIGH;
			if (badMask != 0) {
				// Extract the number of valid bytes before the first special character
				final var safeBytes = (Long.numberOfTrailingZeros(badMask) >>> 3);
				sOff   += safeBytes;
				dstOff += safeBytes;
				// Inline processing of the special character
				final var c = (int) (word >>> (safeBytes << 3)) & 0xFF;
				final var entry = JSONString.LATIN1_TABLE[c]; // Array lookup (O(1)) and sOff increment
				JSONString.LONG_VIEW.set(dst, dstOff, entry);       // Write 8 bytes (payload + garbage + length byte)
				dstOff += (int) (entry >>> 56);                     // Shift offset by actual length
				sOff++;
			} else {
				// Everything is safe ASCII, all 8 bytes are used
				sOff   += 8;
				dstOff += 8;
			}
		}
		return latin1Tail(src, sOff, sLen, dst, dstOff, dstLen, startS, startD);
	}

	static long latin1Tail(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen, final int startS, final int startD) {
		final var remaining = sLen - sOff;

		// --- SHORT STRING FAST-PATH ---
		// Fängt winzige Strings ("id", "key") und den Rest von langen Strings ab
		if (remaining > 0 && remaining < 8 && dstOff <= dstLen - 8) {
			var word = 0L;
			var customMask = 0L;

			// 1. Exaktes Laden ohne Array-Out-Of-Bounds (Branchless auf x86 via Jump Table)
			switch (remaining) {
			case 1: word = src[sOff] & 0xFFL; customMask = 0x80L; break;
			case 2: word =  (short) SHORT_VIEW.get(src, sOff) & 0xFFFFL; customMask = 0x8080L; break;
			case 3: word = ((short) SHORT_VIEW.get(src, sOff) & 0xFFFFL) | ((src[sOff + 2] & 0xFFL) << 16); customMask = 0x808080L; break;
			case 4: word =  (int  ) INT_VIEW  .get(src, sOff) & 0xFFFFFFFFL; customMask = 0x80808080L; break;
			case 5: word = ((int  ) INT_VIEW  .get(src, sOff) & 0xFFFFFFFFL) | ((src[sOff + 4] & 0xFFL) << 32); customMask = 0x8080808080L; break;
			case 6: word = ((int  ) INT_VIEW  .get(src, sOff) & 0xFFFFFFFFL) | (((short)SHORT_VIEW.get(src, sOff + 4) & 0xFFFFL) << 32); customMask = 0x808080808080L; break;
			case 7: word = ((int  ) INT_VIEW  .get(src, sOff) & 0xFFFFFFFFL) | (((short)SHORT_VIEW.get(src, sOff + 4) & 0xFFFFL) << 32) | ((src[sOff + 6] & 0xFFL) << 48); customMask = 0x80808080808080L; break;
			}

			// 2. SWAR Check mit passgenauer Maske! (Schneidet False Positives ab)
			final var quoteXor     = word ^ JSONString.QUOTE_PATTERN;
			final var hasQuote     = (quoteXor     - JSONString.SWARN) & ~quoteXor;
			final var backslashXor = word ^ JSONString.BACKSLASH_PATTERN;
			final var hasBackslash = (backslashXor - JSONString.SWARN) & ~backslashXor;
			final var hasControl   = ~((word + JSONString.CONTROL_PATTERN) | word);
			final var badMask      = (hasQuote | hasBackslash | word | hasControl) & customMask;

			if (badMask == 0) {
				// Reiner ASCII-Erfolg! Schreibe spekulativ 8 Bytes und setze die Pointer exakt.
				JSONString.LONG_VIEW.set(dst, dstOff, word);
				dstOff += remaining;
				sOff += remaining;
				return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
			}
		}

		// --- SCALAR CLEANUP ---
		// Wird nur noch betreten, wenn ein winziger String tatsächlich ein Escape enthält!
		while (sOff < sLen && dstOff <= dstLen - 8) {
			final var entry = JSONString.LATIN1_TABLE[src[sOff++] & 0xFF];
			JSONString.LONG_VIEW.set(dst, dstOff, entry);
			dstOff += (int) (entry >>> 56);
		}
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}

	static long encodeUTF16(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitD = dstLen - 16; // Increased buffer for 8-byte accumulator flushes
		final var safeLimitS = sLen - 4;

		// --- SHORT STRING GUARD ---
		// If the string is smaller than 4 characters, we mathematically cannot do a safe 8-byte read.
		// Route it directly to the highly optimized Jump-Table tail!
		if (safeLimitS < 0) {
			return utf16Tail(src, sOff, sLen, dst, dstOff, dstLen, startS, startD);
		}

		while (sOff < sLen && dstOff < safeLimitD) {
			// --- 1. SWAR FAST-PATH ---
			while (sOff <= safeLimitS && dstOff <= safeLimitD) {
				final var word = (long) JSONString.LONG_VIEW.get(src, sOff << 1);

				final var quoteXor     = word ^ UTF16_QUOTE_PATTERN;
				final var backslashXor = word ^ UTF16_BACKSLASH_PATTERN;
				final var hasQuote     = (quoteXor     - UTF16_SWARN) & ~quoteXor;
				final var hasBackslash = (backslashXor - UTF16_SWARN) & ~backslashXor;
				final var hasControl   = ~((word + UTF16_CONTROL) | word) & UTF16_CONTROL_MASK;
				final var badMask      = (hasQuote | hasBackslash | word | hasControl) & UTF16_NON_ASCII_PATTERN;

				if (badMask != 0) {
					final var safeChars = (Long.numberOfTrailingZeros(badMask) >>> 4);
					if(safeChars > 0) {
						INT_VIEW.set(dst, dstOff, (int) (((word & 0x00FF00FF00FF00FFL) * MAGIC_4X16_to_4X8) >>> 48));
						sOff   += safeChars;
						dstOff += safeChars;
					}
					final var c = (char) (word >>> (safeChars << 4));
					if (c <= 255) {
						// Sliding Window: Process Latin1 escapes inline
						final var entry = JSONString.LATIN1_TABLE[c];
						JSONString.LONG_VIEW.set(dst, dstOff, entry);
						dstOff += (int) (entry >>> 56);
						sOff++;
						continue; // Back to SWAR!
					}
					// True Unicode (> 255) -> Exit to accumulator!
					break;
				}

				// Pure ASCII success
				INT_VIEW.set(dst, dstOff, (int) (word  * MAGIC_4X16_to_4X8) >>> 48);
				sOff   += 4;
				dstOff += 4;
			}

			// Loop exit condition for tail
			if (sOff >= sLen || dstOff >= safeLimitD) break;

			// --- 2. UNICODE ACCUMULATOR FALLBACK ---
			var acc = 0L;
			var accLen = 0;

			// --- YOUR OPTIMIZATION: Register-batching for the fallback ---
			var wordAcc = 0L;
			var charsInWord = 0;

			while (sOff < sLen && dstOff < safeLimitD) {
				// Accumulator Flush
				if (accLen > 4) {
					JSONString.LONG_VIEW.set(dst, dstOff, acc);
					dstOff += accLen;
					acc = 0;
					accLen = 0;
				}

				// --- 8-Byte Overlapping Read ---
				if (charsInWord == 0) {
					var skip = safeLimitS - sOff;
					if (skip > 0) skip = 0; // We have enough space, no skip needed

					// Safe 8-byte read, even at the very last character of the array!
					wordAcc = (long) JSONString.LONG_VIEW.get(src, (sOff + skip) << 1);

					if (skip < 0) {
						// We are at the end. We read 'skip' characters too far to the left.
						// We just shift these old characters out of the register!
						wordAcc >>>= (-skip << 4); // -skip * 16 Bits
					charsInWord = 4 + skip;    // 4 minus the overlapped characters remain
					} else {
						charsInWord = 4;
					}
				}

				// Extract the character in 1 clock cycle directly from the register!
				final var c = (char) wordAcc;
				wordAcc = (wordAcc >>> 16);
				charsInWord--;
				// ---------------------------------------------

				if (c <= 255) {
					// 1. Flush (we must do this in any case)
					if (accLen > 0) {
						JSONString.LONG_VIEW.set(dst, dstOff, acc);
						dstOff += accLen;
						acc = 0;
						accLen = 0;
					}

					// 2. Routing: Back to SWAR or inline tail processing?
					if (sOff <= safeLimitS) {
						break; // We still have space, let SWAR process the ASCII characters!
					}

					// We are in the tail! Process the Latin1 character directly here, otherwise infinite loop!
					final var entry = JSONString.LATIN1_TABLE[c];
					JSONString.LONG_VIEW.set(dst, dstOff, entry);
					dstOff += (int) (entry >>> 56);
					sOff++;

				} else if (Character.isHighSurrogate(c) && sOff + 1 < sLen) {
					// For surrogate pairs, we briefly fallback to the array to keep the logic simple
					final var lowPos = (sOff + 1) << 1;
					final var low = (char) ((src[lowPos] & 0xFF) | ((src[lowPos + 1] & 0xFF) << 8));

					if (Character.isLowSurrogate(low)) {
						final var cp = Character.toCodePoint(c, low);
						final var b1 = (byte) (0xF0 | (cp >> 18)) & 0xFFL;
						final var b2 = (byte) (0x80 | ((cp >> 12) & 0x3F)) & 0xFFL;
						final var b3 = (byte) (0x80 | ((cp >> 6) & 0x3F)) & 0xFFL;
						final var b4 = (byte) (0x80 | (cp & 0x3F)) & 0xFFL;
						acc |= (b1 | (b2 << 8) | (b3 << 16) | (b4 << 24)) << (accLen << 3);
						accLen += 4;
						sOff += 2;
						charsInWord = 0; // Force a new 8-byte load in the next iteration
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
			}

			// Final accumulator flush before the end or return
			if (accLen > 0) {
				JSONString.LONG_VIEW.set(dst, dstOff, acc);
				dstOff += accLen;
			}
		}
		return utf16Tail(src, sOff, sLen, dst, dstOff, dstLen, startS, startD);
	}

	static long utf16Tail(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen, final int startS, final int startD) {
		final var remaining = sLen - sOff;

		// --- SHORT STRING FAST-PATH ---
		if (remaining > 0 && remaining < 4 && dstOff <= dstLen - 8) {
			var word = 0L;
			var customMask = 0L;

			switch (remaining) {
			case 1: word = (short) SHORT_VIEW.get(src, sOff << 1) & 0xFFFFL; customMask = 0xFF80L; break;
			case 2: word = (int) INT_VIEW.get(src, sOff << 1) & 0xFFFFFFFFL; customMask = 0xFF80FF80L; break;
			case 3: word = ((int) INT_VIEW.get(src, sOff << 1) & 0xFFFFFFFFL) | (((short)SHORT_VIEW.get(src, (sOff << 1) + 4) & 0xFFFFL) << 32); customMask = 0xFF80FF80FF80L; break;
			}

			final var quoteXor     = word ^ UTF16_QUOTE_PATTERN;
			final var backslashXor = word ^ UTF16_BACKSLASH_PATTERN;
			final var hasQuote     = (quoteXor     - UTF16_SWARN) & ~quoteXor;
			final var hasBackslash = (backslashXor - UTF16_SWARN) & ~backslashXor;
			final var hasControl   = ~((word + UTF16_CONTROL) | word) & UTF16_CONTROL_MASK;
			final var badMask      = (hasQuote | hasBackslash | word | hasControl) & customMask;

			if (badMask == 0) {
				// Compress UTF-16 to Latin1 und spekulativer Write!
				INT_VIEW.set(dst, dstOff, (int) (word * MAGIC_4X16_to_4X8 >>> 48));
				dstOff += remaining;
				sOff += remaining;
				return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
			}
		}

		// --- SCALAR CLEANUP ---
		while (sOff < sLen && dstOff <= dstLen - 8) {
			final var bytePos = sOff << 1;
			final var c = (char) ((src[bytePos] & 0xFF) | ((src[bytePos + 1] & 0xFF) << 8));
			sOff++;

			if (c <= 255) {
				final var entry = JSONString.LATIN1_TABLE[c];
				JSONString.LONG_VIEW.set(dst, dstOff, entry);
				dstOff += (int) (entry >>> 56);
			} else if (Character.isHighSurrogate(c) && sOff < sLen) {
				final var low = (char) ((src[sOff << 1] & 0xFF) | ((src[(sOff << 1) + 1] & 0xFF) << 8));
				if (Character.isLowSurrogate(low)) {
					final var cp = Character.toCodePoint(c, low);
					dst[dstOff++] = (byte) (0xF0 | (cp >> 18));
					dst[dstOff++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
					dst[dstOff++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
					dst[dstOff++] = (byte) (0x80 | (cp & 0x3F));
					sOff++;
				} else {
					dst[dstOff++] = '?';
				}
			} else {
				if (c >= 0x800) {
					dst[dstOff++] = (byte) (0xE0 | (c >> 12));
					dst[dstOff++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				} else {
					dst[dstOff++] = (byte) (0xC0 | (c >> 6));
				}
				dst[dstOff++] = (byte) (0x80 | (c & 0x3F));
			}
		}
		return (((long) (sOff - startS)) << 32) | ((dstOff - startD) & 0xFFFFFFFFL);
	}
}