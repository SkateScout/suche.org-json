package org.suche.json;

import java.io.IOException;

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

	int parseNumberCore(final byte[] buffer, int pos, final int limit) throws IOException {
		var lIntDigitCount = 0;
		var lFracDigits    = 0;
		var lNumberVal     = 0L;
		var lIsNegative    = false;
		var lParsedExp     = 0;
		var lVirtualExp    = 0;
		var lExpNeg        = false;

		final var limitSafe = limit;
		if (pos < limitSafe && buffer[pos] == '-') {
			lIsNegative = true;
			pos++;
		}

		// 2. Strict JSON check: No leading zeros (z.B. "01", "00" abfangen)
		if (pos < limitSafe && buffer[pos] == '0') {
			pos++;
			// Wenn nach der '0' eine weitere Ziffer kommt -> Exception!
			if (pos < limitSafe && buffer[pos] >= '0' && buffer[pos] <= '9') throwInvalid("Leading zeros are not allowed");
			lIntDigitCount = 1;
			lNumberVal = 0;
		} else {
			// Integer Part (SWAR)
			while (pos <= limitSafe - 8) {
				final var word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				final var val  = word - 0x3030303030303030L;
				// Branchless Check für 0-9
				final var non_digits = (val | (val + 0x0606060606060606L)) & 0xF0F0F0F0F0F0F0F0L;
				if (non_digits == 0) {
					if (lIntDigitCount + 8 <= 18) {
						var chunk = (val & 0x00FF00FF00FF00FFL) * 10 + ((val >>> 8) & 0x00FF00FF00FF00FFL);
						chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
						chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
						lNumberVal = lNumberVal * 100000000L + chunk;
						lIntDigitCount += 8;
						pos += 8;
						continue;
					}
				} else {
					// WICHTIG: Ermittelt exakt, wie viele gültige Bytes wir haben!
					final var len = Long.numberOfTrailingZeros(non_digits) >>> 3;
					if (len > 0 && lIntDigitCount + len <= 18) {
						// Ein einziger Shift entfernt den Müll und füllt mit "führenden Nullen" auf
						var chunk = val << (64 - (len << 3));

						// Die originale SWAR-Multiplikation!
						chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
						chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
						chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);

						// Shift der bisherigen Zahl + unser berechneter Chunk
						lNumberVal = lNumberVal * POW10_L[len] + chunk;
						lIntDigitCount += len;
						pos += len;
					}
				}
				break; // Stop-Zeichen gefunden (Punkt, Komma, e), abbrechen!
			}

			// Scalar Fallback für das Puffer-Ende oder Überlauf
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				if (++lIntDigitCount <= 18) {
					lNumberVal = lNumberVal * 10L + d;
				} else {
					lVirtualExp++;
				}
				pos++;
			}
			if (lIntDigitCount == 0) throwInvalid("Missing integer digits");
		}

		// Fractional Part (After the dot)
		if (pos < limitSafe && buffer[pos] == '.') {
			pos++; // Skip the dot

			while (pos <= limitSafe - 8) {
				final var word = (long) JSONString.LONG_VIEW.get(buffer, pos);
				final var val  = word - 0x3030303030303030L;
				final var non_digits = (val | (val + 0x0606060606060606L)) & 0xF0F0F0F0F0F0F0F0L;

				if (non_digits == 0) {
					if (lIntDigitCount + lFracDigits + 8 <= 18) {
						var chunk = (val & 0x00FF00FF00FF00FFL) * 10 + ((val >>> 8) & 0x00FF00FF00FF00FFL);
						chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
						chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);

						lNumberVal = lNumberVal * 100000000L + chunk;
						lFracDigits += 8;
						pos += 8;
						continue;
					}
				} else {
					final var len = Long.numberOfTrailingZeros(non_digits) >>> 3;
					if (len > 0 && lIntDigitCount + lFracDigits + len <= 18) {
						var chunk = val << (64 - (len << 3));
						chunk = (chunk & 0x00FF00FF00FF00FFL) * 10 + ((chunk >>> 8) & 0x00FF00FF00FF00FFL);
						chunk = (chunk & 0x0000FFFF0000FFFFL) * 100 + ((chunk >>> 16) & 0x0000FFFF0000FFFFL);
						chunk = (chunk & 0x00000000FFFFFFFFL) * 10000 + (chunk >>> 32);
						lNumberVal = lNumberVal * POW10_L[len] + chunk;
						lFracDigits += len;
						pos += len;
					}
				}
				break;
			}

			// Scalar Fallback
			while (pos < limitSafe) {
				final var d = buffer[pos] - '0';
				if (d < 0 || d > 9) break;
				if (lIntDigitCount + lFracDigits < 18) {
					lFracDigits++;
					lNumberVal = lNumberVal * 10L + d;
				}
				pos++;
			}
			if (lFracDigits == 0 && lIntDigitCount <= 18) {
				throwInvalid("Missing fractional digits");
			}
		}

		// Exponent Part ('e' or 'E')
		if (pos < limitSafe && (buffer[pos] == 'e' || buffer[pos] == 'E')) {
			pos++;
			if (pos < limitSafe) {
				final var sign = buffer[pos];
				if (sign == '-') { lExpNeg = true; pos++; }
				else if (sign == '+') { pos++; }
			}
			var expDigits = 0;
			while (pos < limitSafe) {
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
		final var sigDigits = lIntDigitCount + lFracDigits;
		if (this.totalExp > 0 && sigDigits + this.totalExp <= 18) {
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
			// Using POW10_L avoids double-to-long casting overhead
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