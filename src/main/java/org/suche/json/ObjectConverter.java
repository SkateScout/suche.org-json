package org.suche.json;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

public final class ObjectConverter {

	private ObjectConverter() { }

	private static <T> ObjectMeta meta(final Map<String, Object> source, final Class<T> targetType, final InternalEngine engine) {
		final var meta = engine.metaOf(targetType);
		if (meta == null) throw new IllegalArgumentException("No metadata for " + targetType.getName());
		// Handle sealed interfaces/classes first by resolving the concrete type
		if (meta.metaType != ObjectMeta.TYPE_SEALED) return meta;
		// Extract the discriminator to determine the actual subclass
		var classVal = source.get(SealedUnionMapper.CLASS_KEY);
		if (classVal == null) classVal = source.get(SealedUnionMapper.ENUM_KEY);
		if (!(classVal instanceof final String className))
			throw new IllegalArgumentException("Missing or invalid discriminator for sealed type " + targetType.getName());
		// Resolve the concrete class from the permitted array
		if (meta.permitted != null)
			for (final var c : meta.permitted)
				if (c.getSimpleName().equals(className) || c.getName().equals(className)) return engine.metaOf(c);
		throw new IllegalArgumentException("Unknown subclass: " + className + " for " + targetType.getName());
	}

	/** Converts a generic Map into a strongly typed POJO or Record using the pre-cached ObjectMeta. */
	@SuppressWarnings("unchecked")
	public static <T> T fromMap(final Map<String, Object> source, final Class<T> targetType) throws Throwable {
		final var engine = (InternalEngine)JsonEngine.DEFAULT;
		if (source == null) return null;
		final var meta = meta(source, targetType, engine);
		if (meta.metaType != ObjectMeta.TYPE_INSTANTIATOR) throw new IllegalArgumentException("Unsupported " + targetType.getName());

		// Route to optimized implementation for zero-allocation primitive extraction if source is a CompactMap
		if (source instanceof final CompactMap cm) {
			return fromCompactMap(cm, meta);
		}

		return fromDefaultMap(source, meta);
	}

	/** Optimized processing for standard Map implementations. */
	@SuppressWarnings("unchecked")
	private static <T> T fromDefaultMap(final Map<String, Object> source, final ObjectMeta meta) throws Throwable {
		// Allocate arrays for constructor arguments locally for virtual thread safety
		final var objs = new Object[meta.components.length];
		final var prims = meta.needsPrims ? new long[meta.components.length] : null;

		for (final var entry : source.entrySet()) {
			final var idx = meta.prepareKey(null, entry.getKey());
			if (idx >= 0) {
				var val = entry.getValue();
				// Handle nested maps recursively
				if (val instanceof final Map<?, ?> m && meta.components[idx].type() != Object.class && meta.components[idx].type() != Map.class)
					val = fromMap((Map<String, Object>) m, meta.components[idx].type());

				// Handle nested collections recursively
				else if (val instanceof final Collection<?> c && meta.components[idx].type().isArray())
					val = fromCollection(c, meta.components[idx].type());
				else if (val instanceof final Boolean b) {
					objs[idx] = b;
					if (prims != null) prims[idx] = b ? 1L : 0L;
				}

				// Assign primitive values directly to avoid boxing overhead in the target constructor
				if (meta.components[idx].type().isPrimitive()) {
					if (val instanceof final Number n) {
						var primVal = 0L;
						if      (meta.components[idx].type() == double.class) primVal = Double.doubleToRawLongBits(n.doubleValue());
						else if (meta.components[idx].type() == float.class ) primVal = Float.floatToRawIntBits(n.floatValue());
						else                                                  primVal = n.longValue();

						if (prims == null) JsonEngine.illegalStateException("Missing prims");
						else prims[idx] = primVal;
					}
				} else {
					// Resolve enums if the target field expects an enum
					if (meta.enumConstants != null && meta.enumConstants[idx] != null && val != null)
						val = Meta.resolveEnum(meta.enumConstants[idx], val);
					objs[idx] = val;
				}
			}
		}
		return (T) meta.factory.create(objs, prims);
	}

	/** Highly optimized path for CompactMap that extracts primitives directly without allocation or boxing. */
	@SuppressWarnings("unchecked")
	private static <T> T fromCompactMap(final CompactMap source, final ObjectMeta meta) throws Throwable {
		final var objs = new Object[meta.components.length];
		final var prims = meta.needsPrims ? new long[meta.components.length] : null;

		final var rawData = source.getRawData();
		final var rawPrims = source.prims();
		final var singleType = source.singleType();

		for (var i = 0; i < rawData.length - 1; i += 2) {
			final var key = rawData[i];
			if (key == null) continue;

			final var targetIdx = meta.prepareKey(null, (String) key);
			if (targetIdx >= 0) {
				final var compactIdx = i >> 1;
				final var targetType = meta.components[targetIdx].type();

				Object val = null;
				var isLong = false;
				var isDouble = false;
				var primBits = 0L;

				switch (singleType) {
				case PRIMITIVE.T_EMPTY -> { }
				case PRIMITIVE.T_LONG -> {
					isLong = true;
					primBits = rawPrims[compactIdx];
				}
				case PRIMITIVE.T_DOUBLE -> {
					isDouble = true;
					primBits = rawPrims[compactIdx];
				}
				default -> { // T_MIXED
					val = rawData[(compactIdx << 1) + 1];
					if (val == PRIMITIVE.LONG) {
						isLong = true;
						primBits = rawPrims[compactIdx];
						val = null;
					} else if (val == PRIMITIVE.DOUBLE) {
						isDouble = true;
						primBits = rawPrims[compactIdx];
						val = null;
					}
				}
				}

				if (targetType.isPrimitive()) {
					if (prims == null) JsonEngine.illegalStateException("Missing prims");
					if (isLong) {
						if      (targetType == double.class) prims[targetIdx] = Double.doubleToRawLongBits(primBits);
						else if (targetType == float.class)  prims[targetIdx] = Float.floatToRawIntBits(primBits);
						else                                 prims[targetIdx] = primBits;
					} else if (isDouble) {
						if      (targetType == double.class) prims[targetIdx] = primBits;
						else if (targetType == float.class)  prims[targetIdx] = Float.floatToRawIntBits((float) Double.longBitsToDouble(primBits));
						else                                 prims[targetIdx] = (long) Double.longBitsToDouble(primBits);
					} else if (val instanceof final Number n) {
						if      (targetType == double.class) prims[targetIdx] = Double.doubleToRawLongBits(n.doubleValue());
						else if (targetType == float.class)  prims[targetIdx] = Float.floatToRawIntBits(n.floatValue());
						else                                 prims[targetIdx] = n.longValue();
					} else if (val instanceof final Boolean b) {
						objs[targetIdx] = b;
						prims[targetIdx] = b ? 1L : 0L;
					}
				} else {
					if (isLong) {
						val = Long.valueOf(primBits);
					} else if (isDouble) {
						val = Double.valueOf(Double.longBitsToDouble(primBits));
					}

					// Handle nested maps recursively
					if (val instanceof final Map<?, ?> m && targetType != Object.class && targetType != Map.class) {
						val = fromMap((Map<String, Object>) m, targetType);
					}
					// Handle nested collections recursively
					else if (val instanceof final Collection<?> c && targetType.isArray()) {
						val = fromCollection(c, targetType);
					}

					// Resolve enums if the target field expects an enum
					if (meta.enumConstants != null && meta.enumConstants[targetIdx] != null && val != null) {
						val = Meta.resolveEnum(meta.enumConstants[targetIdx], val);
					}
					objs[targetIdx] = val;
				}
			}
		}
		return (T) meta.factory.create(objs, prims);
	}

	/** Converts a generic Collection into a typed Array. */
	@SuppressWarnings("unchecked")
	private static <T> T[] fromCollection(final Collection<?> source, final Class<?> typ) throws Throwable {
		if (source == null) return null;
		final var compType = typ.getComponentType();
		final var result   = Array.newInstance(compType, source.size());
		var i = 0;
		for (final var item : source) {
			if (item instanceof final Map<?, ?> m && compType != Object.class && compType != Map.class)
				Array.set(result, i++, fromMap((Map<String, Object>) m, compType));
			else JsonEngine.illegalStateException("Unsupported Type: "+compType.getCanonicalName());
		}
		return (T[])result;
	}
}