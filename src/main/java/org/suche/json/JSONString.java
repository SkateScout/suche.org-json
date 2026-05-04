package org.suche.json;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class JSONString {
	static final byte[]     hex                     = "0123456789abcdef".getBytes();
	static final VarHandle  LONG_VIEW               = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
	static final long       QUOTE_PATTERN           = 0x2222222222222222L; // '"' in every byte
	static final long       BACKSLASH_PATTERN       = 0x5C5C5C5C5C5C5C5CL; // '\' in every byte
	static final long       NON_ASCII_PATTERN       = 0x8080808080808080L; // Sign-Bit in every byte
	static final long       SWARN_MASK_HIGH         = 0x8080808080808080L; // Sign-Bit in every byte
	static final long       CONTROL_PATTERN         = 0x6060606060606060L;
	static final long       SWARN                   = 0x0101010101010101L;

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

	sealed interface JSONStringProvider permits JSONStringVanilla, JSONStringAddOpens, JSONStringVectorAPI {
		// Returns (sOff << 32) | dstOff
		long encodeChunk(String s, int sOff, int sLen, byte[] dst, int dstOff, int dstLen);
	}

	private static final JSONStringProvider PROVIDER;

	static {
		JSONStringProvider selected = null;
		// Check for Add-Opens Version

		try {
			// Lädt die Klasse nur, wenn die JVM mit --enable-preview gestartet wurde
			// und die JDK Version EXAKT zur Kompilier-Version passt.
			final var clazz = Class.forName("org.suche.json.JSONStringVectorAPI");
			selected = (JSONStringProvider) clazz.getDeclaredConstructor().newInstance();
		} catch (final Throwable t) {
			// Catch Throwable fängt UnsupportedClassVersionError (Minor 65535) oder NoClassDefFoundError ab.
		}
		if(selected == null) {
			if(JSONStringAddOpens.ONCE != null) selected = JSONStringAddOpens.ONCE;
			// Fallback to Vanilla (always works)
			else                                selected = JSONStringVanilla.INSTANCE;
		}
		System.out.println("\n\n\nJSONString using "+selected.getClass().getCanonicalName()+"\n\n\n");
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