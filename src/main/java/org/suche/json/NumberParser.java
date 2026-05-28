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

		var start = (int)JSONStringAddOpens.INT_VIEW.get(buffer, pos);

		var stopChar = (byte) start;
		if (stopChar == '-') {
			lIsNegative = true;
			pos++;
			start>>=8;
			stopChar = (byte) start;
		}

		if (stopChar == '0') {
			pos++;
			start>>=8;
			stopChar = (byte) start;
			if (stopChar >= '0' && stopChar <= '9') throwInvalid("Leading zeros are not allowed");
			lIntDigitCount = 1;
		}

		var sigDigits = lIntDigitCount;
		var phaseFraction = false;
		while (true) {
			byte stopCharFound = -1;
			// STAGE 1: THE HOT LOOP (Garantiert sicher vor Overflow!)
			while (sigDigits <= 10) {
				final var word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				final var val  = word - 0x3030303030303030L;
				final var non_digits = (val | (val + 0x0606060606060606L)) & 0xF0F0F0F0F0F0F0F0L;

				// --- FAST PATH: Keine Branches, keine TrailingZeros ---
				if (non_digits == 0) {
					var chunk = val;
					chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
					chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
					chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
					lNumberVal = lNumberVal * 100000000L + chunk; // Konstante statt POW10_L Array-Lookup!
					sigDigits += 8;
					pos += 8;
					continue; // C2 liebt diesen klaren Loop-Sprung
				}

				// --- EXIT PATH ---
				final var len = Long.numberOfTrailingZeros(non_digits) >>> 3;
				if (len > 0) {
					var chunk = val << (64 - (len << 3));
					chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
					chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
					chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
					lNumberVal = lNumberVal * POW10_L[len] + chunk;
					sigDigits += len;
					pos += len;
				}

				stopCharFound = (byte) (word >>> (len << 3));
				break;
			}

			// ====================================================================
			// STAGE 2: THE OVERFLOW LOOP (Deine Logik, komplett isoliert!)
			// Wird NUR betreten, wenn die Zahl gigantisch ist (> 10 Ziffern)
			// ====================================================================
			while (stopCharFound == -1) {
				final var word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				final var val  = word - 0x3030303030303030L;
				final var non_digits = (val | (val + 0x0606060606060606L)) & 0xF0F0F0F0F0F0F0F0L;
				final var len = Long.numberOfTrailingZeros(non_digits) >>> 3;

				if (sigDigits + len > 18) {
					// --- DEIN REGISTER DRAIN ---
					final var fill = 18 - sigDigits;
					for (var i = 0; i < fill; i++) lNumberVal = lNumberVal * 10L + ((val >>> (i << 3)) & 0xFF);
					sigDigits = 18;

					final var overflow = len - fill;
					lVirtualExp += overflow;
					pos += len;

					if (len < 8) {
						stopCharFound = (byte) (word >>> (len << 3));
						break;
					}

					// Memory-Scalar Fallback, falls die Zahl absurderweise > 26 Ziffern hat
					while (true) {
						final var d = buffer[pos];
						if (d < '0' || d > '9') {
							stopCharFound = d;
							break;
						}
						lVirtualExp++;
						pos++;
					}
					break;
				}

				// Es passt doch noch genau rein (z.B. sigDigits war 12, len ist 4 -> 16 <= 18)
				if (len > 0) {
					var chunk = val << (64 - (len << 3));
					chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
					chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
					chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
					lNumberVal = lNumberVal * POW10_L[len] + chunk;
					sigDigits += len;
					pos += len;
				}

				if (len < 8) {
					stopCharFound = (byte) (word >>> (len << 3));
					break;
				}
			}

			// ====================================================================
			// PHASE TRANSITION LOGIC
			// ====================================================================
			if (!phaseFraction) {
				lIntDigitCount = sigDigits;
				if (lIntDigitCount == 0) throwInvalid("Missing integer digits");

				if (stopCharFound == '.') {
					phaseFraction = true;
					pos++;
					continue; // Springt hoch und macht nahtlos beim Fraction-Teil weiter!
				}
			} else {
				lFracDigits = sigDigits - lIntDigitCount;
				if (lFracDigits == 0) throwInvalid("Missing fractional digits");
			}
			stopChar = stopCharFound;

			break;
		}

		if (stopChar == 'e' || stopChar == 'E') {
			final var sign = buffer[++pos];
			switch (sign) {
			case '-' -> { lExpNeg = true; pos++; }
			case '+' -> pos++;
			}

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