package org.suche.json;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class JSONString {
	static final byte[]     hex               = "0123456789abcdef".getBytes();
	static final VarHandle  LONG_VIEW               = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
	static final long       QUOTE_PATTERN           = 0x2222222222222222L; // '"' in every byte
	static final long       BACKSLASH_PATTERN       = 0x5C5C5C5C5C5C5C5CL; // '\' in every byte
	static final long       NON_ASCII_PATTERN       = 0x8080808080808080L; // Sign-Bit in every byte
	static final long       SWARN                   = 0x0101010101010101L;

	sealed interface JSONStringProvider permits JSONStringVanilla, JSONStringAddOpens {
		// Returns (sOff << 32) | dstOff
		long encodeChunk(String s, int sOff, int sLen, byte[] dst, int dstOff, int dstLen);
	}

	private static final JSONStringProvider PROVIDER;

	static {
		JSONStringProvider selected;
		try { selected = new JSONStringAddOpens(); } // 2. Test for Add-Opens access to String.value
		catch (final RuntimeException _) { selected = JSONStringVanilla.INSTANCE; } // 3. Fallback to Vanilla (always works)
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
}