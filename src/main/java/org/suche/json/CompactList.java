package org.suche.json;

import java.util.AbstractList;
import java.util.RandomAccess;

public final class CompactList extends AbstractList<Object> implements ContextBacked,RandomAccess, JSONArray {
	private final Object[] data;
	private final long  [] prims;
	private final byte     singleType;
	private       int      removed = 0;

	@Override public void put(final int index, final int val) {
		if (index < 0) throw JsonEngine.illegalStateException("Index can not be negative");
		if(index == size() && removed > 0) removed--;
		if (index >= size()) throw JsonEngine.illegalStateException("Index: " + index + ", Size: " + size());
		switch (singleType) {
		case MetaPool.T_EMPTY  ->   JsonEngine.illegalStateException("Cannot modify empty list");
		case MetaPool.T_LONG   ->   prims[index] = val;
		case MetaPool.T_DOUBLE -> { prims[index] = Double.doubleToRawLongBits(val); }
		default -> { data[index] = val; if(prims!=null) prims[index] = val; }
		}
	}

	@Override public Object set(final int index, final Object element) {
		if (index < 0) throw JsonEngine.illegalStateException("Index can not be negative");
		if(index == size() && removed > 0) removed--;
		if (index >= size()) JsonEngine.illegalStateException("Index: " + index + ", Size: " + size());
		final var oldValue = resolve(index);
		switch (singleType) {
		case MetaPool.T_EMPTY  -> JsonEngine.illegalStateException("Cannot modify empty list");
		case MetaPool.T_LONG   -> {
			if (!(element instanceof final Number n)) throw JsonEngine.illegalStateException("Strict T_LONG list requires a Number");
			if(n.longValue() != n.doubleValue()) throw JsonEngine.illegalStateException("Strict T_LONG fast frac check");
			prims[index] = n.longValue();
		}
		case MetaPool.T_DOUBLE -> {
			if (!(element instanceof final Number n)) throw JsonEngine.illegalStateException("Strict T_DOUBLE list requires a Number");
			prims[index] = Double.doubleToRawLongBits(n.doubleValue());
		}
		default -> {
			switch(element) {
			case final Long   l -> { data[index] = l; if(prims!=null) prims[index] = l; }
			case final Double d -> { data[index] = d; if(prims!=null) prims[index] = Double.doubleToRawLongBits(d); }
			case null,default   -> data[index] = element;
			}
		}
		}
		return oldValue;
	}

	@Override public void removeByIndex(final int index) {
		removed++;
		if(data  != null) System.arraycopy(data , index+1, data , index,data .length-index-removed);
		if(prims != null) System.arraycopy(prims, index+1, prims, index,prims.length-index-removed);
	}

	@Override public boolean  isEmpty   () {
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> true;
		case MetaPool.T_LONG   -> prims.length-removed==0;
		case MetaPool.T_DOUBLE -> prims.length-removed==0;
		default                -> data .length-removed==0;
		};
	}

	@Override public int      size      () {
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> 0;
		case MetaPool.T_LONG   -> prims.length - removed;
		case MetaPool.T_DOUBLE -> prims.length - removed;
		default                -> data .length - removed;
		};
	}

	@Override public Object get       (final int index) { return resolve(index); }
	Object[] getRawData() { return data; }
	@Override public Object   rawValueAt(final int logicalIdx) { return data[logicalIdx]; }
	@Override public long  [] prims     () { return prims     ; }
	@Override public byte     singleType() { return singleType; }

	/** Provides compatibility with org.json array length logic */
	@Override public int      length    () { return size(); }

	private Object resolve (final int valIndex) {
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> JsonEngine.illegalStateException("empty");
		case MetaPool.T_LONG   -> Long  .valueOf(                        prims[valIndex] );
		case MetaPool.T_DOUBLE -> Double.valueOf(Double.longBitsToDouble(prims[valIndex]));
		default                -> {
			final var val = data[valIndex];
			if (val == PRIMITIVE.LONG)   yield Long  .valueOf(                        prims[valIndex] );
			if (val == PRIMITIVE.DOUBLE) yield Double.valueOf(Double.longBitsToDouble(prims[valIndex]));
			yield val;
		}
		};
	}

	// Safely attempts to retrieve a string, returning the fallback on out-of-bounds or null
	@Override public String optString(final int index, final String fallback) {
		if(index < 0) return fallback;
		return switch(singleType) {
		case MetaPool.T_EMPTY  -> fallback;
		case MetaPool.T_LONG   -> index>=prims.length?fallback:Long  .toString(                        prims[index] );
		case MetaPool.T_DOUBLE -> index>=prims.length?fallback:Double.toString(Double.longBitsToDouble(prims[index]));
		default                -> {
			if(index>=prims.length) yield fallback;
			final var val = data[index];
			if(val == null) yield fallback;
			if (val == PRIMITIVE.LONG)   yield Long  .toString(                        prims[index] );
			if (val == PRIMITIVE.DOUBLE) yield Double.toString(Double.longBitsToDouble(prims[index]));
			yield data[index].toString();
		}
		};
	}

	// Safely retrieves a nested JSON object at the given index
	@Override public JSONObject optJSONObject(final int index) { return(data != null && index < data.length && data[index] instanceof final JSONObject m ? m : null); }

	// Safely retrieves a nested JSON array at the given index
	@Override public JSONArray optJSONArray  (final int index) { return(data != null && index < data.length && data[index] instanceof final JSONArray  l  ? l : null); }

	// Safely retrieves a nested JSON object at the given index
	@Override public JSONObject getJSONObject(final int index) {
		if (optJSONObject(index) instanceof final JSONObject m) return m;
		throw JsonEngine.illegalStateException("Value at index [" + index + "] is not a JSONObject");
	}

	// Safely retrieves a nested JSON array at the given index
	@Override public JSONArray getJSONArray  (final int index) {
		if (optJSONArray(index) instanceof final JSONArray l) return l;
		throw JsonEngine.illegalStateException("Value at index [" + index + "] is not a JSONArray");
	}

	// Retrieves the string representation of the value at the given index
	@Override public String getString(final int index) { return optString(index, null); }

	CompactList(final byte pSingleType, final Object[] pData, final long[] pPrims) {
		this.singleType = pSingleType;
		this.data       = pData;
		this.prims      = pPrims;
	}
}