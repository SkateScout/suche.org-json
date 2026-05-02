package org.suche.json;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

public final class CompactMap<K, V> extends AbstractMap<K, V> implements ConextBacked<V> {
	public static final class PRIMITIVE extends Number {
		private static final long serialVersionUID = 1L;
		private PRIMITIVE() { }
		public static final PRIMITIVE LONG   = new PRIMITIVE();
		public static final PRIMITIVE DOUBLE = new PRIMITIVE();
		@Override public int    intValue   () { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public long   longValue  () { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public float  floatValue () { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
		@Override public double doubleValue() { throw new UnsupportedOperationException("Unresolved lazy primitive"); }
	}
	private final Object[]             data;
	private final long[]               prims;
	private final byte                 singleType;
	private       Set<K>               keys; // LAZY INIT
	private       Set<Map.Entry<K, V>> entrySet;

	@Override public Object rawValueAt(final int logicalIdx) { return data[(logicalIdx << 1) + 1]; }
	@Override public long  [] prims     () { return prims     ; }
	@Override public byte     singleType() { return singleType; }

	Object[] getRawData() { return data; }

	CompactMap(final byte pSingleType, final Object[] pData, final long[] pPrims) {
		this.singleType = pSingleType;
		this.data       = pData;
		this.prims      = pPrims;
		for(var i=0;i < pData.length; i += 2) if(pData[i] == null) throw new IllegalArgumentException("Missing key "+i/2+" "+Arrays.toString(pData));
	}

	@SuppressWarnings("unchecked")
	@Override public Set<K> keySet() {
		if(keys == null) {
			keys = new TreeSet<>();
			for(var i=0;i < data.length; i += 2) if(data[i] != null) keys.add((K)data[i]);
		}
		return keys;
	}

	@Override public int size() { return data.length >> 1; }

	@SuppressWarnings("unchecked")
	private V resolve(final int valIndex) {
		final var val = data[valIndex];
		if (val == PRIMITIVE.LONG)   return (V) Long.valueOf(prims[valIndex >> 1]);
		if (val == PRIMITIVE.DOUBLE) return (V) Double.valueOf(Double.longBitsToDouble(prims[valIndex >> 1]));
		return (V) val;
	}

	@Override public V get(final Object key) {
		for (var i = 0; i < data.length - 1; i += 2) if (key.equals(data[i])) return resolve(i + 1);
		return null;
	}

	@Override public boolean containsKey(final Object key) {
		for (var i = 0; i < data.length; i += 2) if (key == data[i] || key.equals(data[i])) return true;
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
							final var v = resolve(index + 1);
							index += 2;
							return new AbstractMap.SimpleImmutableEntry<>(k, v);
						}
					};
				}
			};
		}
		return entrySet;
	}

	@Override
	public void forEach(final BiConsumer<? super K, ? super V> action) {
		if (action == null) throw new NullPointerException();
		for (var i = 0; i < data.length - 1; i += 2) {
			@SuppressWarnings("unchecked")
			final var k = (K) data[i];
			action.accept(k, resolve(i + 1));
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

	private int idx(final Object key) {
		for (var i = 0; i < data.length - 1; i += 2) if (key == data[i] || key.equals(data[i])) return(i >> 1);
		return -1;
	}

	public long      getLong   (final Object key) { return getLong   (idx(key)); }
	public int       getInteger(final Object key) { return getInteger(idx(key)); }
	public short     getShort  (final Object key) { return getShort  (idx(key)); }
	public byte      getByte   (final Object key) { return getByte   (idx(key)); }
	public boolean   getBoolean(final Object key) { return getBoolean(idx(key)); }
	public char      getChar   (final Object key) { return getChar   (idx(key)); }
	public double    getDouble (final Object key) { return getDouble (idx(key)); }
	public float     getFloat  (final Object key) { return getFloat  (idx(key)); }

	public long      optLong   (final Object key, final long    fallback) { return optLong   (idx(key), fallback); }
	public int       optInteger(final Object key, final int     fallback) { return optInteger(idx(key), fallback); }
	public short     optShort  (final Object key, final short   fallback) { return optShort  (idx(key), fallback); }
	public byte      optByte   (final Object key, final byte    fallback) { return optByte   (idx(key), fallback); }
	public boolean   optBoolean(final Object key, final boolean fallback) { return optBoolean(idx(key), fallback); }
	public char      optChar   (final Object key, final char    fallback) { return optChar   (idx(key), fallback); }
	public double    optDouble (final Object key, final float   fallback) { return optDouble (idx(key), fallback); }
	public float     optFloat  (final Object key, final double  fallback) { return optFloat  (idx(key), fallback); }

	public Long      optLong   (final Object key) { return optLong   (idx(key)); }
	public Integer   optInteger(final Object key) { return optInteger(idx(key)); }
	public Short     optShort  (final Object key) { return optShort  (idx(key)); }
	public Byte      optByte   (final Object key) { return optByte   (idx(key)); }
	public Boolean   optBoolean(final Object key) { return optBoolean(idx(key)); }
	public Character optChar   (final Object key) { return optChar   (idx(key)); }
	public Double    optDouble (final Object key) { return optDouble (idx(key)); }
	public Float     optFloat  (final Object key) { return optFloat  (idx(key)); }
}