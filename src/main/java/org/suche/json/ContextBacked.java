package org.suche.json;

sealed interface ContextBacked permits CompactList, CompactMap, JSONArray {
	int      size      ();
	long  [] prims     ();
	byte     singleType();
	Object   rawValueAt(int logicalIdx);

	default long      getLong   (final int idx) {
		final var prims      = prims     ();
		final var singleType = singleType();
		return switch(singleType) {
		case PRIMITIVE.T_LONG   -> {
			if(idx < 0 || idx >= size()) JsonEngine.illegalStateException("Index "+idx+" not in [0...S:"+size()+"<C:"+(prims.length-1)+"]");
			yield prims[idx];
		}
		case PRIMITIVE.T_DOUBLE -> {
			if(idx < 0 || idx >= size()) JsonEngine.illegalStateException("Index "+idx+" not in [0...S:"+size()+"<C:"+(prims.length-1)+"]");
			yield (long)Double.longBitsToDouble(prims[idx]);
		}
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) {
				if(idx < 0 || idx >= size()) JsonEngine.illegalStateException("Index "+idx+" not in [0...S:"+size()+"<C:"+(prims.length-1)+"]");
				yield prims[idx];
			}
			if(data == PRIMITIVE.DOUBLE) {
				if(idx < 0 || idx >= size()) JsonEngine.illegalStateException("Index "+idx+" not in [0...S:"+size()+"<C:"+(prims.length-1)+"]");
				yield (long)Double.longBitsToDouble(prims[idx]);
			}
			if(data instanceof final Number t) yield t.longValue();
			throw JsonEngine.illegalStateException("Index["+idx+"] nonPrimitive");
		}
		};
	}

	default Number  optNumber (final int idx, final Number fallback) {
		final var prims      = prims     ();
		final var singleType = singleType();
		if(idx < 0 || idx >= size()) return fallback;
		return switch(singleType) {
		case PRIMITIVE.T_LONG   -> prims[idx];
		case PRIMITIVE.T_DOUBLE -> Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.longValue();
			yield fallback;
		}
		};
	}

	default Long      oLong   (final int idx) {
		if(idx < 0) return null;
		final var prims      = prims     ();
		if(idx >= size()) throw new IndexOutOfBoundsException();
		final var singleType = singleType();
		return switch(singleType) {
		case PRIMITIVE.T_LONG   -> prims[idx];
		case PRIMITIVE.T_DOUBLE -> (long)Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield (long)Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.longValue();
			yield null;
		}
		};
	}

	default long      optLong   (final int idx, final long    fallback) { final var v = oLong  (idx); return(v==null?fallback:v); }
	default double    optDouble (final int idx, final double  fallback) { final var v = oDouble(idx); return(v==null?fallback:v); }

	default Double    oDouble (final int idx) {
		if(idx < 0) return null;
		final var prims      = prims     ();
		final var singleType = singleType();
		return switch(singleType) {
		case PRIMITIVE.T_LONG   -> (double)prims[idx];
		case PRIMITIVE.T_DOUBLE -> Double.longBitsToDouble(prims[idx]);
		default -> {
			final var data = rawValueAt(idx);
			if(data == PRIMITIVE.LONG  ) yield (double)prims[idx];
			if(data == PRIMITIVE.DOUBLE) yield Double.longBitsToDouble(prims[idx]);
			if(data instanceof final Number t) yield t.doubleValue();
			yield null;
		}
		};
	}
}