package org.suche.json;

import java.util.List;

/**
 * A lightweight interface representing a JSON array, optimized for read-heavy operations.
 */
public sealed interface JSONArray extends List<Object>, ContextBacked permits CompactList, EmptyJSONArray {

	/** Provides compatibility with org.json array length logic. */
	int length();

	// --- Object & Array Retrieval ---

	/** Safely retrieves a nested JsonObject at the given index. Throws ClassCastException if type mismatches. */
	JSONObject getJSONObject(int index);

	/** Safely retrieves a nested JsonObject, returning null if missing or not an object. */
	JSONObject optJSONObject(int index);

	/** Safely retrieves a nested JsonArray at the given index. Throws ClassCastException if type mismatches. */
	JSONArray getJSONArray(int index);

	/** Safely retrieves a nested JsonArray, returning null if missing or not an array. */
	JSONArray optJSONArray(int index);

	// --- String Retrieval ---

	String getString(int index);
	String optString(int index, String fallback);
	default String optString(final int index) { return optString(index, null); }

	default int          getInt    (final int idx) { return (int  )                        getLong(idx); }
	default int          getInteger(final int idx) { return (int  )                        getLong(idx); }
	default short        getShort  (final int idx) { return (short)                        getLong(idx); }
	default byte         getByte   (final int idx) { return (byte )                        getLong(idx); }
	default boolean      getBoolean(final int idx) { return (                              getLong(idx) != 0); }
	default char         getChar   (final int idx) { return (char )                        getLong(idx); }
	default double       getDouble (final int idx) { return        Double.longBitsToDouble(getLong(idx)); }
	default float        getFloat  (final int idx) { return (float)Double.longBitsToDouble(getLong(idx)); }

	default int          optInt    (final int idx, final int     fallback) { return (int  ) optLong  (idx, fallback); }
	default int          optInteger(final int idx, final int     fallback) { return (int  ) optLong  (idx, fallback); }
	default short        optShort  (final int idx, final short   fallback) { return (short) optLong  (idx, fallback); }
	default byte         optByte   (final int idx, final byte    fallback) { return (byte ) optLong  (idx, fallback); }
	default boolean      optBoolean(final int idx, final boolean fallback) { return (       optLong  (idx, fallback?1:0) != 0); }
	default char         optChar   (final int idx, final char    fallback) { return (char ) optLong  (idx, fallback); }
	default float        optFloat  (final int idx, final float   fallback) { return (float) optDouble(idx, fallback); }

	default int          optInt    (final int idx) { final var v = oLong  (idx); return(null==v?0:v.intValue()); }
	default int          optInteger(final int idx) { final var v = oLong  (idx); return(null==v?0:v.intValue()); }
	default short        optShort  (final int idx) { final var v = oLong  (idx); return(null==v?0:v.shortValue()); }
	default byte         optByte   (final int idx) { final var v = oLong  (idx); return(null==v?0:v.byteValue()); }
	default boolean      optBoolean(final int idx) { final var v = oLong  (idx); return(null!=v && v.longValue()!=0); }
	default char         optChar   (final int idx) { final var v = oLong  (idx); return(null==v?0:(char)v.intValue()); }
	default double       optDouble (final int idx) { final var v = oDouble(idx); return(null==v?0:v); }
	default float        optFloat  (final int idx) { final var v = oDouble(idx); return(null==v?0:v.floatValue()); }

	@Override default boolean add(final Object val) { put(size(),val); return true; }
	@Override default void    add(final int idx, final Object val) { add(idx,val); }
	default void put(final int index, final Object val) { set(index,val); }

	void removeByIndex(final int index);

	default List<Object> toList    () { return this; }
}