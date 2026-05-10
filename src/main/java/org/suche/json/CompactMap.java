package org.suche.json;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;

public final class CompactMap extends AbstractMap<String, Object> implements ContextBacked, JSONObject {
	private Object[]                  data;
	private long  []                  prims;
	private byte                      singleType;
	private Set<String>               keys; // LAZY INIT
	private Set<Map.Entry<String, Object>> entrySet;

	@Override public Object rawValueAt(final int logicalIdx) { return data[(logicalIdx << 1) + 1]; }
	@Override public long  [] prims     () { return prims     ; }
	@Override public byte     singleType() { return singleType; }

	Object[] getRawData() { return data; }

	public CompactMap() { this(4); }

	public CompactMap(final int initialKeys) {
		this.singleType = MetaPool.T_EMPTY;
		final var cap = Math.max(initialKeys, 4);
		this.data = new Object[cap << 1];
	}

	CompactMap(final byte pSingleType, final Object[] pData, final long[] pPrims) {
		this.singleType = pSingleType;
		this.data       = pData;
		this.prims      = pPrims;
		for(var i=0;i < pData.length; i += 2) if(pData[i] == null) throw new IllegalArgumentException("Missing key "+i/2+" "+Arrays.toString(pData));
	}

	@Override
	public Set<String> keySet() {
		if (keys == null) {
			// Creates a lightweight view over the existing array without allocating node objects
			keys = new AbstractSet<>() {
				@Override public int size() { return CompactMap.this.size(); }
				@Override public Iterator<String> iterator() {
					return new Iterator<>() {
						private int index = 0;
						@Override public boolean hasNext() {
							while(index < data.length && data[index]==null) index += 2;
							return index < data.length;
						}
						@Override public String next() {
							while(index < data.length && data[index]==null) index += 2;
							if (index >= data.length - 1) throw new NoSuchElementException();
							final var k = (String) data[index];
							index += 2;
							return k;
						}
					};
				}
			};
		}
		return keys;
	}

	/** Checks if the key exists within the map */
	@Override public boolean has(final String pKey) { return idx(pKey)>=0; }

	// Verifies if the value associated with the key is explicitly null
	@Override public boolean isNull(final String pKey) {
		final var idx = idx(pKey);
		if(idx < 0) return true;
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> true;
		case MetaPool.T_LONG   -> false;
		case MetaPool.T_DOUBLE -> false;
		default                -> null != rawValueAt(idx);
		};
	}

	/** Safely retrieves a nested JSON object utilizing pattern matching for performance */
	@Override
	public CompactMap getJSONObject(final String pKey) {
		if (optJSONObject(pKey) instanceof final CompactMap m) return m;
		throw new ClassCastException("Value for key '" + pKey + "' is not a JSONObject");
	}

	/** Safely retrieves a nested JSON object utilizing pattern matching for performance */
	@Override
	public CompactMap optJSONObject(final String pKey) {
		final var idx = idx(pKey);
		return (idx >= 0 && rawValueAt(idx) instanceof final CompactMap m ? (CompactMap)m : null);
	}


	// Safely retrieves a nested JSON array
	@Override public JSONArray getJSONArray(final String pKey) {
		if(optJSONArray(pKey) instanceof final JSONArray l) return l;
		throw new ClassCastException("Value for key '" + pKey + "' is not a JSONArray");
	}

	// Safely retrieves a nested JSON array
	@Override public JSONArray optJSONArray(final String pKey) {
		final var idx = idx(pKey);
		return (idx >= 0 && rawValueAt(idx) instanceof final JSONArray c ? c : null);
	}

	// Retrieves the string representation of the value
	@Override public String getString(final String pKey) { return optString(pKey, null); }

	// Retrieves the string value or returns the provided fallback if missing
	@Override public String optString(final String pKey, final String fallback) {
		final var idx = idx(pKey);
		if(idx < 0) return fallback;
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> fallback;
		case MetaPool.T_LONG   -> Long  .toString(                        prims[idx] );
		case MetaPool.T_DOUBLE -> Double.toString(Double.longBitsToDouble(prims[idx]));
		default                -> {
			final var val = rawValueAt(idx);
			if (val == null) yield fallback;
			if (val == PRIMITIVE.LONG)   yield Long  .toString(                        prims[idx] );
			if (val == PRIMITIVE.DOUBLE) yield Double.toString(Double.longBitsToDouble(prims[idx]));
			yield val.toString();
		}
		};
	}

	@Override public int length() { return size(); }

	private Object resolve(final int idx) {
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> null;
		case MetaPool.T_LONG   -> Long  .valueOf(                        prims[idx] );
		case MetaPool.T_DOUBLE -> Double.valueOf(Double.longBitsToDouble(prims[idx]));
		default                -> {
			final var val = rawValueAt(idx);
			if (val == PRIMITIVE.LONG)   yield Long.valueOf(prims[idx]);
			if (val == PRIMITIVE.DOUBLE) yield Double.valueOf(Double.longBitsToDouble(prims[idx]));
			yield val;
		}
		};
	}

	@Override public Object get(final Object key) {
		final var idx = idx((String)key);
		if(idx < 0) return null;
		return (idx >= 0 ? resolve(idx) : null);
	}

	@Override public Object opt(final String key) {
		final var idx = idx(key);
		return (idx >= 0 ? resolve(idx) : null);
	}

	@Override public boolean containsKey(final Object key) { return idx((String)key)>=0; }

	@Override public Set<Map.Entry<String, Object>> entrySet() {
		if (entrySet == null) {
			entrySet = new AbstractSet<>() {
				@Override public int size() { return data.length >> 1; }
				@Override public Iterator<Map.Entry<String, Object>> iterator() {
					return new Iterator<>() {
						private int index = 0;
						@Override public boolean hasNext() { return index < data.length; }
						@Override public Map.Entry<String, Object> next() {
							if (index >= data.length - 1) throw new NoSuchElementException();
							final var k = (String) data[index];
							final var v = resolve(index>>1);
							index += 2;
							return new AbstractMap.SimpleImmutableEntry<>(k, v);
						}
					};
				}
			};
		}
		return entrySet;
	}

	@Override public int hashCode() {
		final var prime = 31;
		var result = super.hashCode();
		result = prime * result + Arrays.deepHashCode(data);
		return prime * result + Arrays.hashCode(prims);
	}

	@Override public boolean equals(final Object obj) {
		if (this == obj) return true;
		if ((obj == null) || !super.equals(obj) || (getClass() != obj.getClass())) return false;
		final var other = (CompactMap) obj;
		return Arrays.deepEquals(data, other.data) && Arrays.equals(prims, other.prims);
	}

	@Override public int size() {
		if (data == null) return 0;
		var count = 0;
		for (var i = 0; i < data.length; i += 2) if (data[i] != null) count++;
		return count;
	}

	private int idx(final String key) {
		if(singleType == MetaPool.T_EMPTY || data == null) return -1;
		for (var i = 0; i < data.length - 1; i += 2) if (key == data[i] || key.equals(data[i])) return(i >> 1);
		return -1;
	}

	@Override public Object remove(final String key) {
		final var idx = idx(key);
		final var val = resolve(idx);
		data[ idx << 1     ] = null; // Key freigeben
		data[(idx << 1) + 1] = null; // Value-Referenz entfernen
		return val;
	}

	@Override public Integer   optInteger(final String key, final Integer fallback) { return(oLong(idx(key)) instanceof final Long l?l.intValue():fallback); }

	@Override public Object    get(final String key) { return resolve(idx(key)); }
	@Override public int       getInt    (final String key) { return (int  )                        getLong(idx(key)); }
	@Override public short     getShort  (final String key) { return (short)                        getLong(idx(key)); }
	@Override public byte      getByte   (final String key) { return (byte )                        getLong(idx(key)); }
	@Override public boolean   getBoolean(final String key) { return (                              getLong(idx(key)) != 0); }
	@Override public char      getChar   (final String key) { return (char )                        getLong(idx(key)); }
	@Override public double    getDouble (final String key) { return        Double.longBitsToDouble(getLong(idx(key))); }
	@Override public float     getFloat  (final String key) { return (float)Double.longBitsToDouble(getLong(idx(key))); }

	@Override public int       optInt    (final String key, final int     fallback) { return (int  ) optLong  (idx(key), fallback); }
	@Override public short     optShort  (final String key, final short   fallback) { return (short) optLong  (idx(key), fallback); }
	@Override public byte      optByte   (final String key, final byte    fallback) { return (byte ) optLong  (idx(key), fallback); }
	@Override public boolean   optBoolean(final String key, final boolean fallback) { return (       optLong  (idx(key), fallback?1:0) != 0); }
	@Override public char      optChar   (final String key, final char    fallback) { return (char ) optLong  (idx(key), fallback); }
	@Override public float     optFloat  (final String key, final float   fallback) { return (float) optDouble(idx(key), fallback); }
	@Override public double    optDouble (final String key, final double  fallback) { final var v = oDouble(idx(key)); return(v==null?fallback:v); }
	@Override public long      optLong   (final String key, final long    fallback) { final var v = oLong(idx(key)); return(v==null?fallback:v); }
	@Override public Number    optNumber (final String key, final Number  fallback) { return (float)optNumber (idx(key), fallback); }

	@Override public int       optInt    (final String key) { final var v = oLong  (idx(key)); return(null==v?0:v.intValue()); }
	@Override public short     optShort  (final String key) { final var v = oLong  (idx(key)); return(null==v?0:v.shortValue()); }
	@Override public byte      optByte   (final String key) { final var v = oLong  (idx(key)); return(null==v?0:v.byteValue()); }
	@Override public boolean   optBoolean(final String key) { final var v = oLong  (idx(key)); return(null==v?false:v.longValue()!=0); }
	@Override public char      optChar   (final String key) { final var v = oLong  (idx(key)); return(null==v?0:(char)v.intValue()); }
	@Override public double    optDouble (final String key) { final var v = oDouble(idx(key)); return(null==v?0:v); }
	@Override public float     optFloat  (final String key) { final var v = oDouble(idx(key)); return(null==v?0:v.floatValue()); }
	@Override public long      getLong   (final String key) { return getLong   (idx(key)); }

	@Override public void forEach(final BiConsumer<? super String, ? super Object> action) {
		if (action == null) throw new NullPointerException();
		for (var i = 0; i < data.length - 1; i += 2) if(data[i] != null){
			final var k = (String) data[i];
			action.accept(k, resolve(i>>1));
		}
	}

	private JSONObject p(final String key, final Object value) {
		if (singleType == MetaPool.T_EMPTY) singleType = MetaPool.T_MIXED;

		if (singleType == MetaPool.T_LONG) {
			if (!(value instanceof final Number n)) upgradeToMixed();
			else if ((n instanceof Double || n instanceof Float) && n.longValue() != n.doubleValue()) upgradeToDouble();
		} else if ((singleType == MetaPool.T_DOUBLE) && !(value instanceof Number)) upgradeToMixed();

		final var idx = getOrCreateIdx(key);
		if (singleType == MetaPool.T_LONG) {
			if (prims == null) prims = new long[data.length >> 1];
			prims[idx] = ((Number) value).longValue();
		} else if (singleType == MetaPool.T_DOUBLE) {
			if (prims == null) prims = new long[data.length >> 1];
			prims[idx] = Double.doubleToRawLongBits(((Number) value).doubleValue());
		} else {
			switch (value) {
			case final Long l   -> { data[(idx << 1) + 1] = l; if (prims != null) prims[idx] = l; }
			case final Double d -> { data[(idx << 1) + 1] = d; if (prims != null) prims[idx] = Double.doubleToRawLongBits(d); }
			case null, default  ->   data[(idx << 1) + 1] = value;
			}
		}
		return this;
	}

	private void upgradeToDouble() {
		if (singleType != MetaPool.T_LONG) return;
		if (prims != null) for (var i = 0; i < prims.length; i++) if (data[i << 1] != null) prims[i] = Double.doubleToRawLongBits(prims[i]);
		singleType = MetaPool.T_DOUBLE;
	}

	private static final int EXPAND_STEP = 8;

	private void upgradeToMixed() {
		switch (singleType) {
		case MetaPool.T_MIXED  -> { return; }
		case MetaPool.T_LONG   -> { for (var i = 0; i < data.length; i += 2) if (data[i] != null) data[i + 1] = PRIMITIVE.LONG;   }
		case MetaPool.T_DOUBLE -> { for (var i = 0; i < data.length; i += 2) if (data[i] != null) data[i + 1] = PRIMITIVE.DOUBLE; }
		default                -> { break; }
		}
		singleType = MetaPool.T_MIXED;
	}

	private int getOrCreateIdx(final String key) {
		if (data == null) {
			data = new Object[8];
			data[0] = key;
			return 0;
		}
		var freeIdx = -1;
		for (var i = 0; i < data.length; i += 2) {
			if (data[i] == null) { if (freeIdx < 0) freeIdx = i >> 1; }
			else if (key.equals(data[i]))  return i >> 1;
		}
		if (freeIdx >= 0) {
			data[freeIdx << 1] = key;
			return freeIdx;
		}
		final var oldLen = data.length;
		final var newLen = oldLen + EXPAND_STEP;

		data = Arrays.copyOf(data, newLen);
		if (prims != null) prims = Arrays.copyOf(prims, newLen >> 1);
		final var newIdx = (oldLen >> 1);
		data[oldLen] = key;
		return newIdx;
	}

	@Override public JSONObject put(final String key, final long   val) {
		if (singleType == MetaPool.T_EMPTY) singleType = MetaPool.T_LONG;
		final var idx = getOrCreateIdx(key);
		if (singleType == MetaPool.T_LONG) {
			if (prims == null) prims = new long[data.length >> 1];
			prims[idx] = val;
		} else if (singleType == MetaPool.T_DOUBLE) {
			if (prims == null) prims = new long[data.length >> 1];
			prims[idx] = Double.doubleToRawLongBits(val);
		} else { // MIXED
			data[(idx << 1) + 1] = PRIMITIVE.LONG;
			if (prims == null) prims = new long[data.length >> 1];
			prims[idx] = val;
		}
		return this;
	}

	@Override public JSONObject put(final String key, final double val) {
		if (singleType == MetaPool.T_EMPTY) singleType = MetaPool.T_DOUBLE;
		final var idx = getOrCreateIdx(key);
		if (singleType == MetaPool.T_LONG && prims != null) upgradeToDouble();
		if (prims == null) prims = new long[data.length >> 1];
		prims[idx] = Double.doubleToRawLongBits(val);
		return this;
	}

	@Override public Object     put(final String key, final Object value) {
		final var idx = getOrCreateIdx(key);
		final var oldValue = resolve(idx);
		p(key, value);
		return oldValue;
	}

	@Override public void clear() {
		if(data != null) Arrays.fill(data, null);
		singleType = MetaPool.T_EMPTY;
		keys  = null;
	}

	@Override public JSONObject put(final String key, final Map<?,?>[]    val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final String        val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final int           val) { return put(key,(long  )val); }
	@Override public JSONObject put(final String key, final float         val) { return put(key,(double)val); }
	@Override public JSONObject put(final String key, final short         val) { return put(key,(long  )val); }
	@Override public JSONObject put(final String key, final byte          val) { return put(key,(long  )val); }
	@Override public JSONObject put(final String key, final char          val) { return put(key,(long  )val); }
	@Override public JSONObject put(final String key, final Number        val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final JSONArray     val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final Collection<?> val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final Map<?,?>      val) { return p  (key,        val); }
	@Override public JSONObject put(final String key, final boolean       val) { return p  (key,        val); }

	@Override public Map<String, Object> internalMap() { return this; }
	@Override public String toString() { return JsonEngine.toString(this); }
}