package org.suche.json;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public final class CompactMap<K, V> extends AbstractMap<K, V> {
	public static final class PRIMITIVE extends Number {
		private static final long serialVersionUID = 1L;
		private PRIMITIVE() { }
		public static final PRIMITIVE LONG   = new PRIMITIVE();
		public static final PRIMITIVE DOUBLE = new PRIMITIVE();

		@Override public int intValue() { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public long longValue() { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public float floatValue() { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public double doubleValue() { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
	}

	private final Object[] data;
	final long[]   prims;
	private Set<Map.Entry<K, V>> entrySet;

	Object[] getRawData() { return data; }

	CompactMap(final Object[] data, final long[] prims) {
		this.data = data;
		this.prims = prims;
	}

	@Override public int size() { return data.length >> 1; }

	@SuppressWarnings("unchecked")
	private V resolve(final Object val, final int valIndex) {
		if (val == PRIMITIVE.LONG)   return (V) Long.valueOf(prims[valIndex >> 1]);
		if (val == PRIMITIVE.DOUBLE) return (V) Double.valueOf(Double.longBitsToDouble(prims[valIndex >> 1]));
		return (V) val;
	}

	@Override public V get(final Object key) {
		for (var i = 0; i < data.length - 1; i += 2) {
			if (key.equals(data[i])) return resolve(data[i + 1], i + 1);
		}
		return null;
	}

	@Override public boolean containsKey(final Object key) {
		for (var i = 0; i < data.length; i += 2) if (key.equals(data[i])) return true;
		return false;
	}

	@Override public Set<Map.Entry<K, V>> entrySet() {
		if (entrySet == null) {
			entrySet = new AbstractSet<>() {
				@Override public int size() { return data.length >> 1; }
				@Override public Iterator<Map.Entry<K, V>> iterator() {
					return new Iterator<>() {
						private int index = 0;
						@Override public boolean hasNext() { return index < data.length; }
						@Override @SuppressWarnings("unchecked") public Map.Entry<K, V> next() {
							if (index >= data.length - 1) throw new NoSuchElementException();
							final var k = (K) data[index];
							final var v = resolve(data[index + 1], index + 1);
							index += 2;
							return new java.util.AbstractMap.SimpleImmutableEntry<>(k, v);
						}
					};
				}
			};
		}
		return entrySet;
	}

	@Override
	public void forEach(final java.util.function.BiConsumer<? super K, ? super V> action) {
		if (action == null) throw new NullPointerException();
		for (var i = 0; i < data.length - 1; i += 2) {
			@SuppressWarnings("unchecked")
			final var k = (K) data[i];
			final var v = resolve(data[i + 1], i + 1);
			action.accept(k, v);
		}
	}

	@Override
	public int hashCode() {
		final var prime = 31;
		var result = super.hashCode();
		result = prime * result + Arrays.deepHashCode(data);
		return prime * result + Arrays.hashCode(prims);
	}

	@Override public boolean equals(final Object obj) {
		if (this == obj) return true;
		if ((obj == null) || !super.equals(obj) || (getClass() != obj.getClass())) return false;
		final var other = (CompactMap<?,?>) obj;
		return Arrays.deepEquals(data, other.data) && Arrays.equals(prims, other.prims);
	}



	public long      getLong   (final Object key) {
		for (var i = 0; i < data.length - 1; i += 2) if (key.equals(data[i])) {
			if(data[i+1] == PRIMITIVE.LONG  ) return prims[i >> 1];
			if(data[i+1] != PRIMITIVE.DOUBLE) return (long)Double.longBitsToDouble(prims[i >> 1]);
			throw new NullPointerException("Key["+key+"] nonPrimitive");
		}
		throw new NullPointerException("Key["+key+"] unknown");
	}

	public int       getInteger(final Object key) { return (int  )                        getLong(key); }
	public short     getShort  (final Object key) { return (short)                        getLong(key); }
	public byte      getByte   (final Object key) { return (byte )                        getLong(key); }
	public boolean   getBoolean(final Object key) { return (                              getLong(key) != 0); }
	public char      getChar   (final Object key) { return (char )                        getLong(key); }
	public double    getDouble (final Object key) { return        Double.longBitsToDouble(getLong(key)); }
	public float     getFloat  (final Object key) { return (float)Double.longBitsToDouble(getLong(key)); }

	public long      optLong   (final Object key, final long    fallback) {
		for (var i = 0; i < data.length - 1; i += 2) if (key.equals(data[i])) {
			if(data[i+1] == PRIMITIVE.LONG  ) return prims[i >> 1];
			if(data[i+1] != PRIMITIVE.DOUBLE) return (long)Double.longBitsToDouble(prims[i >> 1]);
		}
		return fallback;
	}

	public int       optInteger(final Object key, final int     fallback) { return (int  ) optLong  (key, fallback); }
	public short     optShort  (final Object key, final short   fallback) { return (short) optLong  (key, fallback); }
	public byte      optByte   (final Object key, final byte    fallback) { return (byte ) optLong  (key, fallback); }
	public boolean   optBoolean(final Object key, final boolean fallback) { return (       optLong  (key, fallback?1:0) != 0); }
	public char      optChar   (final Object key, final char    fallback) { return (char ) optLong  (key, fallback); }
	public float     optFloat  (final Object key, final float   fallback) { return (float) optDouble(key, fallback); }
	public double    optDouble (final Object key, final double  fallback) {
		for (var i = 0; i < data.length - 1; i += 2) if (key.equals(data[i])) {
			if(data[i+1] == PRIMITIVE.LONG  ) return prims[i >> 1];
			if(data[i+1] != PRIMITIVE.DOUBLE) return (long)Double.longBitsToDouble(prims[i >> 1]);
		}
		return fallback;
	}

	public Long      optLong   (final Object key) { throw new UnsupportedOperationException(); }
	public Integer   optInteger(final Object key) { throw new UnsupportedOperationException(); }
	public Short     optShort  (final Object key) { throw new UnsupportedOperationException(); }
	public Byte      optByte   (final Object key) { throw new UnsupportedOperationException(); }
	public Boolean   optBoolean(final Object key) { throw new UnsupportedOperationException(); }
	public Character optChar   (final Object key) { throw new UnsupportedOperationException(); }
	public Double    optDouble (final Object key) { throw new UnsupportedOperationException(); }
	public Float     optFloat  (final Object key) { throw new UnsupportedOperationException(); }
}