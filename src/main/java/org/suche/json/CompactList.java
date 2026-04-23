package org.suche.json;

import java.util.AbstractList;
import java.util.RandomAccess;

import org.suche.json.CompactMap.PRIMITIVE;

sealed interface ConextBacked<E> permits CompactList, CompactMap {
	long  [] prims     ();
	byte     singleType();
	Object   rawValueAt(int logicalIdx);

	@SuppressWarnings("unchecked") default E get(final int idx) {
		final var prims      = prims     ();
		final var singleType = singleType();
		return switch(singleType) {
		case MetaPool.T_LONG   -> (E)Long.valueOf(prims[idx]);
		case MetaPool.T_DOUBLE -> (E)Double.valueOf(Double.longBitsToDouble(prims[idx]));
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield (E)Long.valueOf(prims[idx]);
			if(data == PRIMITIVE.DOUBLE) yield (E)Double.valueOf(Double.longBitsToDouble(prims[idx]));
			throw new NullPointerException("Index["+idx+"] nonPrimitive");
		}
		};
	}

	default long      getLong   (final int idx) {
		final var prims      = prims     ();
		final var singleType = singleType();
		if(idx < 0 || idx >= prims.length) throw new IndexOutOfBoundsException();
		return switch(singleType) {
		case MetaPool.T_LONG   -> prims[idx];
		case MetaPool.T_DOUBLE -> (long)Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield (long)Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.longValue();
			throw new NullPointerException("Index["+idx+"] nonPrimitive");
		}
		};
	}

	default long      optLong   (final int idx, final long    fallback) {
		final var prims      = prims     ();
		final var singleType = singleType();
		if(idx < 0 || idx >= prims.length) throw new IndexOutOfBoundsException();
		return switch(singleType) {
		case MetaPool.T_LONG   -> prims[idx];
		case MetaPool.T_DOUBLE -> (long)Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield (long)Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.longValue();
			yield fallback;
		}
		};
	}

	default Long      optLong   (final int idx) {
		final var prims      = prims     ();
		final var singleType = singleType();
		if(idx < 0 || idx >= prims.length) throw new IndexOutOfBoundsException();
		return switch(singleType) {
		case MetaPool.T_LONG   -> prims[idx];
		case MetaPool.T_DOUBLE -> (long)Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield (long)Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.longValue();
			yield null;
		}
		};
	}

	default double    optDouble (final int idx, final double  fallback) {
		final var prims      = prims     ();
		final var singleType = singleType();
		if(idx < 0 || idx >= prims.length) throw new IndexOutOfBoundsException();
		return switch(singleType) {
		case MetaPool.T_LONG   -> prims[idx];
		case MetaPool.T_DOUBLE -> Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data       = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.doubleValue();
			yield fallback;
		}
		};
	}

	default Double    optDouble (final int idx) {
		final var prims      = prims     ();
		final var singleType = singleType();
		return switch(singleType) {
		case MetaPool.T_LONG   -> (double)prims[idx];
		case MetaPool.T_DOUBLE -> Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield (double)prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.doubleValue();
			yield null;
		}
		};
	}

	default int       getInteger(final int idx) { return (int  )                        getLong(idx); }
	default short     getShort  (final int idx) { return (short)                        getLong(idx); }
	default byte      getByte   (final int idx) { return (byte )                        getLong(idx); }
	default boolean   getBoolean(final int idx) { return (                              getLong(idx) != 0); }
	default char      getChar   (final int idx) { return (char )                        getLong(idx); }
	default double    getDouble (final int idx) { return        Double.longBitsToDouble(getLong(idx)); }
	default float     getFloat  (final int idx) { return (float)Double.longBitsToDouble(getLong(idx)); }
	default int       optInteger(final int idx, final int     fallback) { return (int  ) optLong  (idx, fallback); }
	default short     optShort  (final int idx, final short   fallback) { return (short) optLong  (idx, fallback); }
	default byte      optByte   (final int idx, final byte    fallback) { return (byte ) optLong  (idx, fallback); }
	default boolean   optBoolean(final int idx, final boolean fallback) { return (       optLong  (idx, fallback?1:0) != 0); }
	default char      optChar   (final int idx, final char    fallback) { return (char ) optLong  (idx, fallback); }
	default float     optFloat  (final int idx, final float   fallback) { return (float) optDouble(idx, fallback); }
	default Integer   optInteger(final int idx) { final var v = optLong  (idx); return(null==v?null:v.intValue()); }
	default Short     optShort  (final int idx) { final var v = optLong  (idx); return(null==v?null:v.shortValue()); }
	default Byte      optByte   (final int idx) { final var v = optLong  (idx); return(null==v?null:v.byteValue()); }
	default Boolean   optBoolean(final int idx) { final var v = optLong  (idx); return(null==v?null:v.longValue()!=0); }
	default Character optChar   (final int idx) { final var v = optLong  (idx); return(null==v?null:(char)v.intValue()); }
	default Float     optFloat  (final int idx) { final var v = optDouble(idx); return(null==v?null:v.floatValue()); }
}

public final class CompactList<E> extends AbstractList<E> implements ConextBacked<E>,RandomAccess {
	private final Object[] data;
	private final long  [] prims;
	private final byte     singleType;

	@Override public Object rawValueAt(final int logicalIdx) { return data[logicalIdx]; }
	@Override public long  [] prims     () { return prims     ; }
	@Override public byte     singleType() { return singleType; }

	@Override
	@SuppressWarnings("unchecked") public E get(final int index) { return (E) rawValueAt(index); }

	Object[] getRawData() { return data; }

	CompactList(final byte pSingleType, final Object[] data, final long[] prims) {
		this.singleType = pSingleType;
		this.data       = data;
		this.prims      = prims;
	}

	@Override public int size() { return data.length; }
}