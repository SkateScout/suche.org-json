package org.suche.json;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.suche.json.ObjectMeta.ComponentMeta;

record FastKeyTable(byte[][] keys, int[] indices, int mask) {
	// static int computeHash(final byte[] buf, final int off, final int len) {
	// 	var h = 0;
	// 	for (var i = 0; i < len; i++) h = 31 * h + buf[off + i];
	// 	return h;
	// }

	static FastKeyTable build(final ComponentMeta[] components) {
		if (components == null || components.length == 0) return new FastKeyTable(new byte[0][], new int[0], 0);
		var cap = 16;
		while (cap < components.length * 3) cap <<= 1;
		final var keys = new byte[cap][];
		final var indices = new int[cap];
		Arrays.fill(indices, -1);
		final var mask = cap - 1;

		for (var i = 0; i < components.length; i++) {
			final var keyBytes = components[i].name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
			final var h = BufferedStream.computeHash(keyBytes, 0, keyBytes.length);
			var idx = h & mask;
			while (keys[idx] != null) idx = (idx + 1) & mask;
			keys[idx] = keyBytes;
			indices[idx] = i;
		}
		return new FastKeyTable(keys, indices, mask);
	}

	int get(final int hash, final byte[] buffer, final int off, final int len) {
		if (mask == 0) return -1;
		var idx = hash & mask;
		while (keys[idx] != null) {
			final var k = keys[idx];
			if (k.length == len && Arrays.equals(k, 0, len, buffer, off, off + len)) return indices[idx];
			idx = (idx + 1) & mask;
		}
		return -1;
	}

	int get(final String key) {
		final var b = key.getBytes(StandardCharsets.UTF_8);
		final var l = b.length;
		final var h = BufferedStream.computeHash(b, 0, l);
		return get(h,b,0,l);
	}
}