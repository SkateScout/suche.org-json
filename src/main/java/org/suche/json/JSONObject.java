package org.suche.json;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A lightweight interface representing a JSON object, optimized for read-heavy operations.
 */
public sealed interface JSONObject extends Map<String,Object> permits CompactMap {

	/** Checks if the key exists within the JSON object. */
	boolean has(String key);

	/** Verifies if the value associated with the key is explicitly null. */
	boolean isNull(String key);

	// --- Object & Array Retrieval ---

	/** Safely retrieves a nested JsonObject. Throws ClassCastException if the type mismatches. */
	Object get(String key);

	/** Safely retrieves a nested JsonObject. Throws ClassCastException if the type mismatches. */
	JSONObject getJSONObject(String key);

	default JSONObject computeIfAbsentJSONObject(final String key) {
		var r = optJSONObject(key);
		if(r == null) put(key, r = new CompactMap());
		return r;
	}

	/** Safely retrieves a nested JsonObject, returning null if missing or not an object. */
	JSONObject optJSONObject(String key);

	/** Safely retrieves a nested JsonArray. Throws ClassCastException if the type mismatches. */
	JSONArray getJSONArray(String key);

	/** Safely retrieves a nested JsonArray, returning null if missing or not an array. */
	JSONArray optJSONArray(String key);

	// --- String Retrieval ---

	String getString(String key);
	String optString(String key, String fallback);

	// --- Primitive Retrieval (Strict) ---
	// These throw exceptions if the key is missing or cannot be converted.
	Object  opt       (String key);
	long    getLong   (String key);
	int     getInt    (String key);
	short   getShort  (String key);
	byte    getByte   (String key);
	boolean getBoolean(String key);
	char    getChar   (String key);
	double  getDouble (String key);
	float   getFloat  (String key);

	// --- Primitive Retrieval (Optional with Fallback) ---

	Integer optInteger(String key, Integer fallback);
	long    optLong   (String key, long    fallback);
	int     optInt    (String key, int     fallback);
	short   optShort  (String key, short   fallback);
	byte    optByte   (String key, byte    fallback);
	boolean optBoolean(String key, boolean fallback);
	char    optChar   (String key, char    fallback);
	double  optDouble (String key, double  fallback);
	float   optFloat  (String key, float   fallback);
	Number  optNumber (String key, Number  fallback);

	// --- Primitive Retrieval (Optional returning Wrapper/Null) ---

	default String  optString (final String key) { return optString (key, null); }
	default long    optLong   (final String key) { return optLong   (key, 0); }
	default int     optInt    (final String key) { return optInt    (key, 0); }
	default short   optShort  (final String key) { return optShort  (key, (short)0); }
	default byte    optByte   (final String key) { return optByte   (key, (byte)0); }
	default boolean optBoolean(final String key) { return optBoolean(key, false); }
	default char    optChar   (final String key) { return optChar   (key, (char)0); }
	default double  optDouble (final String key) { return optDouble (key, 0.); }
	default float   optFloat  (final String key) { return optFloat  (key, 0); }
	default Number  optNumber (final String key) { return optNumber (key, null); }

	//
	@Override
	Set<String> keySet();
	@Override
	Object      put(String key, Object        val);
	JSONObject  put(String key, String        val);
	JSONObject	put(String key, long          val);
	JSONObject  put(String key, int           val);
	JSONObject  put(String key, short         val);
	JSONObject  put(String key, byte          val);
	JSONObject	put(String key, boolean       val);
	JSONObject	put(String key, char          val);
	JSONObject	put(String key, double        val);
	JSONObject	put(String key, float         val);
	JSONObject	put(String key, Number        val);
	JSONObject  put(String key, JSONArray     val);
	JSONObject	put(String key, Collection<?> val);
	JSONObject	put(String key, Map<?,?>      val);
	JSONObject	put(String key, Map<?,?>[]    val);

	@Override
	void clear();
	Object              remove(String key);
	@Override
	boolean             isEmpty();
	int                 length();

	default Map<String, Object> internalMap()  { return this; }
	default Map<String, Object> toMap() { return this; }

	default String toString(final int i) {
		if(i<0) JsonEngine.illegalStateException("Must positive");
		return JsonEngine.toString(this);
	}
}