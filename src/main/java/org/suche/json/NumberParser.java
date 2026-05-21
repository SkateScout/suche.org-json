package org.suche.json;

final class NumberParser {
	private static final double[]   POW10 = { 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18 };
	private static final long[]     POW10_L = {
			1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
			1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L,
			100000000000000L, 1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L
	};

	private long    numberVal  = 0L;
	private int     totalExp   = 0;
	private boolean isNegative = false;
	private boolean isFloat    = false;

	boolean isFloat  () { return isFloat  ; }
	long    numberVal() { return numberVal; }

	private static void throwInvalid(final String mesg) { throw new IllegalStateException(mesg); }

	int parseNumberCore(final byte[] buffer, int pos) {
		var lIntDigitCount = 0;
		var lFracDigits    = 0;
		var lNumberVal     = 0L;
		var lIsNegative    = false;
		var lParsedExp     = 0;
		var lVirtualExp    = 0;
		var lExpNeg        = false;

		var stopChar = (int) buffer[pos];
		if (stopChar == '-') {
			lIsNegative = true;
			stopChar = buffer[++pos];
		}

		if (stopChar == '0') {
			stopChar = buffer[++pos];
			if (stopChar >= '0' && stopChar <= '9') throwInvalid("Leading zeros are not allowed");
			lIntDigitCount = 1;
		}
		var sigDigits = lIntDigitCount;
		var phaseFraction = false;
		while (true) {
			while (true) {
				final var word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				final var val  = word - 0x3030303030303030L;
				final var non_digits = (val | (val + 0x0606060606060606L)) & 0xF0F0F0F0F0F0F0F0L;
				if (non_digits == 0) {
					if (sigDigits + 8 <= 18) {
						var chunk = (val & 0x00FF00FF00FF00FFL) * 10 + ((val >>> 8) & 0x00FF00FF00FF00FFL);
						chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
						chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
						lNumberVal = lNumberVal * 100000000L + chunk;
						sigDigits += 8;
						pos += 8;
						continue;
					}
				} else {
					final var len = Long.numberOfTrailingZeros(non_digits) >>> 3;
					if (sigDigits + len <= 18) {
						if (len > 0) {
							var chunk = val << (64 - (len << 3));
							chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
							chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
							chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
							lNumberVal = lNumberVal * POW10_L[len] + chunk;
							sigDigits += len;
							pos += len;
						}
						stopChar = (int) ((word >>> (len << 3)) & 0xFF);
						break;
					}
				}
				// FALLBACK: Register Drain & Memory Scalar
				var offset = 0;
				while (offset < 8) {
					final var d = (int) ((word >>> (offset << 3)) & 0xFF) - '0';
					if (d < 0 || d > 9) { stopChar = d + '0'; break; }
					if (++sigDigits <= 18) lNumberVal = lNumberVal * 10L + d;
					else lVirtualExp++;
					offset++;
					pos++;
				}
				if (offset == 8) {
					while (true) {
						final var d = buffer[pos] - '0';
						if (d < 0 || d > 9) { stopChar = d + '0'; break; }
						if (++sigDigits <= 18) lNumberVal = lNumberVal * 10L + d;
						else lVirtualExp++;
						pos++;
					}
				}
				break;
			}

			// --- PHASE TRANSITION LOGIC ---
			if (!phaseFraction) {
				lIntDigitCount = sigDigits;
				if (lIntDigitCount == 0) throwInvalid("Missing integer digits");

				if (stopChar == '.') {
					phaseFraction = true;
					pos++; // Skip DOT
					continue; // Restart parsing fraction part
				}
			} else {
				lFracDigits = sigDigits - lIntDigitCount;
				if (lFracDigits == 0) throwInvalid("Missing fractional digits");
			}

			break;
		}

		// Exponent Part
		if (stopChar == 'e' || stopChar == 'E') {
			pos++;
			final var sign = buffer[pos];
			if (sign == '-') { lExpNeg = true; pos++; }
			else if (sign == '+') { pos++; }

			var expDigits = 0;
			while (true) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				lParsedExp = lParsedExp * 10 + d;
				expDigits++;
				pos++;
			}
			if (expDigits == 0) throwInvalid("Missing exponent digits");
		}

		this.totalExp = (lExpNeg ? -lParsedExp : lParsedExp) + lVirtualExp - lFracDigits;

		// Fast-Path Normalize
		if (this.totalExp > 0 && lIntDigitCount + lFracDigits + this.totalExp <= 18) {
			lNumberVal *= POW10_L[this.totalExp];
			this.totalExp = 0;
		}

		this.numberVal  = lNumberVal;
		this.isNegative = lIsNegative;
		this.isFloat    = (this.totalExp != 0);
		return pos;
	}
	// ---------------------------------------------------------
	// Math calculation helpers to prevent double-parsing
	// ---------------------------------------------------------

	double computeDoubleValue() {
		double d = numberVal;
		if (totalExp != 0) {
			if (totalExp > 0 && totalExp < POW10.length) {
				d *= POW10[totalExp];
			} else if (totalExp < 0 && -totalExp < POW10.length) {
				d /= POW10[-totalExp];
			} else {
				d *= Math.pow(10, totalExp);
			}
		}
		return isNegative ? -d : d;
	}

	long computeLongValue() {
		var v = numberVal;
		if (totalExp != 0) {
			if (totalExp > 0 && totalExp < POW10_L.length) {
				v *= POW10_L[totalExp];
			} else if (totalExp < 0 && -totalExp < POW10_L.length) {
				v /= POW10_L[-totalExp];
			} else {
				v = (long) (v * Math.pow(10, totalExp));
			}
		}
		return isNegative ? -v : v;
	}
}