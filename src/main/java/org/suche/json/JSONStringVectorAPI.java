package org.suche.json;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import org.suche.json.JSONString.JSONStringProvider;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

final class JSONStringVectorAPI implements JSONStringProvider {

	private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
	private static final int V_LEN = SPECIES.length();

	private static final VectorSpecies<Short> SPECIES_SHORT = ShortVector.SPECIES_PREFERRED;
	private static final int V_LEN_SHORT = SPECIES_SHORT.length();
	// Die Byte-Species für das Downcasting (16 shorts -> 16 bytes)
	private static final VectorSpecies<Byte> SPECIES_BYTE_NARROW = SPECIES_SHORT.withLanes(byte.class);

	static long encodeLatin1(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS     = sOff;
		final var startD     = dstOff;
		final var safeLimitS = sLen   - V_LEN;
		final var safeLimitD = dstLen - (V_LEN * 2); // Mehr Puffer für den breiteren Vector
		while (sOff <= safeLimitS && dstOff <= safeLimitD) {
			// Load 32 (oder 64) Bytes in einem einzigen Takt!
			final var v = ByteVector.fromArray(SPECIES, src, sOff);
			//Build Hardware-Masken for all special characters
			final var mQuote = v.eq((byte) '"');
			final var mSlash = v.eq((byte) '\\');
			// Control Characters (< 32)
			final var mCtrl  = v.compare(VectorOperators.ULT, (byte) 32);
			// Non-ASCII (> 127). Da Java-Bytes signed sind, sind Werte > 127 negativ (< 0)
			final var mNonAsc = v.compare(VectorOperators.LT, (byte) 0);
			// Combine mask
			final var badMask = mQuote.or(mSlash).or(mCtrl).or(mNonAsc);
			if (!badMask.anyTrue()) {
				v.intoArray(dst, dstOff);
				sOff   += V_LEN;
				dstOff += V_LEN;
			} else {
				final var safeBytes = badMask.firstTrue();
				v.intoArray(dst, dstOff);
				sOff   += safeBytes;
				dstOff += safeBytes;
				final var entry = JSONString.LATIN1_TABLE[src[sOff++] & 0xFF];
				JSONString.LONG_VIEW.set(dst, dstOff, entry);
				dstOff += (int) (entry >>> 56);
			}
		}
		return JSONStringAddOpens.latin1Tail(src, sOff, sLen, dst, dstOff, dstLen, startS, startD);
	}

	static long encodeUTF16(final byte[] src, int sOff, final int sLen, final byte[] dst, int dstOff, final int dstLen) {
		final var startS = sOff;
		final var startD = dstOff;
		final var safeLimitD = dstLen - Math.max(V_LEN_SHORT, 6);

		// NEU: Wrappe das byte-Array in ein MemorySegment für die Vector API
		final var srcSeg = MemorySegment.ofArray(src);

		while (sOff < sLen && dstOff < safeLimitD) {
			final var remainingS = sLen - sOff;
			final var remainingD = safeLimitD - dstOff;
			final var runLimit = sOff + (Math.min(remainingS, remainingD) & ~(V_LEN_SHORT - 1));

			// --- SIMD FAST-PATH ---
			while (sOff < runLimit) {
				// NEU: Lade die 16-Bit UTF-16 Zeichen aus dem MemorySegment
				final var v = ShortVector.fromMemorySegment(SPECIES_SHORT, srcSeg, sOff << 1, ByteOrder.LITTLE_ENDIAN);

				final var mQuote  = v.eq((short) '"');
				final var mSlash  = v.eq((short) '\\');
				final var mCtrl   = v.compare(VectorOperators.ULT, (short) 32);
				final var mNonAsc = v.compare(VectorOperators.UGT, (short) 127);

				final var badMask = mQuote.or(mSlash).or(mCtrl).or(mNonAsc);

				if (badMask.anyTrue()) {
					final var safeChars = badMask.firstTrue();
					if (safeChars > 0) {
						final var narrowed = (ByteVector) v.castShape(SPECIES_BYTE_NARROW, 0);
						narrowed.intoArray(dst, dstOff);
						sOff   += safeChars;
						dstOff += safeChars;
					}
					break; // Raus in den skalaren Fallback
				}
				// Hardware Downcasting von UTF-16 auf Latin1 (Truncate)
				final var narrowed = (ByteVector) v.castShape(SPECIES_BYTE_NARROW, 0);
				narrowed.intoArray(dst, dstOff);
				sOff   += V_LEN_SHORT;
				dstOff += V_LEN_SHORT;
			}

			// 2. Skalarer Fallback
			if (sOff < sLen && dstOff < safeLimitD) {
				final var bytePos = sOff << 1;
				final var c = (char) ((src[bytePos] & 0xFF) | ((src[bytePos + 1] & 0xFF) << 8));
				sOff++;
				if (c <= 255) {
					final var entry = JSONString.LATIN1_TABLE[c];
					JSONString.LONG_VIEW.set(dst, dstOff, entry);
					dstOff += (int) (entry >>> 56);
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

	@Override public long encodeChunk(final String s, final int sOff, final int sLen, final byte[] dst, final int dstOff, final int dstLen) {
		final var coder = (byte) JSONStringAddOpens.CODER_HANDLE.get(s);
		if (coder == JSONStringAddOpens.LATIN1) {
			final var value = (byte[]) JSONStringAddOpens.VALUE_HANDLE.get(s);
			return encodeLatin1(value, sOff, sLen, dst, dstOff, dstLen);
		}
		if (coder == JSONStringAddOpens.UTF16 ) {
			final var value = (byte[]) JSONStringAddOpens.VALUE_HANDLE.get(s);
			return             encodeUTF16(value, sOff, sLen, dst, dstOff, dstLen);
		}
		// Fallback to a UTF16-aware logic or the vanilla logic
		return JSONStringVanilla.INSTANCE.encodeChunk(s, sOff, sLen, dst, dstOff, dstLen);
	}
}