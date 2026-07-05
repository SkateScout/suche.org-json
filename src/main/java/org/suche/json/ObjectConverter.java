package org.suche.json;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class ObjectConverter {

	private ObjectConverter() { }

	private static <T> ObjectMeta meta(final Map<String, Object> source, final Class<T> targetType, final InternalEngine engine) {
		final var meta = engine.metaOf(targetType);
		if (meta == null) throw new IllegalArgumentException("No metadata for " + targetType.getName());
		if (meta.metaType != ObjectMeta.TYPE_SEALED) return meta;
		var classVal = source.get(SealedUnionMapper.CLASS_KEY);
		if (classVal == null) {
			for (final String alias : new String[]{"type", "__type__", "@type", SealedUnionMapper.ENUM_KEY}) {
				classVal = source.get(alias);
				if (classVal != null) break;
			}
		}
		if (!(classVal instanceof final String className))
			throw new IllegalArgumentException("Missing or invalid discriminator for sealed type " + targetType.getName());
		if (meta.permitted != null)
			for (final var c : meta.permitted)
				if (c.getSimpleName().equals(className) || c.getName().equals(className)) return engine.metaOf(c);
		throw new IllegalArgumentException("Unknown subclass: " + className + " for " + targetType.getName());
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromMap(final Map<String, Object> source, final Class<T> targetType) throws Throwable {
		final var engine = (InternalEngine)JsonEngine.DEFAULT;
		if (source == null) return null;
		final var meta = meta(source, targetType, engine);
		if (meta.metaType != ObjectMeta.TYPE_INSTANTIATOR) throw new IllegalArgumentException("Unsupported " + targetType.getName());

		if (source instanceof final CompactMap cm) {
			return fromCompactMap(cm, meta);
		}

		return fromDefaultMap(source, meta);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fromDefaultMap(final Map<String, Object> source, final ObjectMeta meta) throws Throwable {
		final var objs = new Object[meta.components.length];
		final var prims = meta.needsPrims ? new long[meta.components.length] : null;

		for (final var entry : source.entrySet()) {
			final var idx = meta.prepareKey(null, entry.getKey());
			if (idx >= 0) {
				var val = entry.getValue();
				final var targetType = meta.components[idx].type();

				if (val instanceof final Map<?, ?> m && targetType != Object.class && targetType != Map.class)
					val = fromMap((Map<String, Object>) m, targetType);
				else if (val instanceof final Collection<?> c && (targetType.isArray() || Collection.class.isAssignableFrom(targetType)))
					val = fromCollection(c, meta.components[idx]);
				else if (val instanceof final Boolean b) {
					objs[idx] = b;
					if (prims != null) prims[idx] = b ? 1L : 0L;
				}

				if (targetType.isPrimitive()) {
					if (val instanceof final Number n) {
						var primVal = 0L;
						if      (targetType == double.class) primVal = Double.doubleToRawLongBits(n.doubleValue());
						else if (targetType == float.class ) primVal = Float.floatToRawIntBits(n.floatValue());
						else                                 primVal = n.longValue();

						if (prims == null) JsonEngine.illegalStateException("Missing prims");
						else prims[idx] = primVal;
					}
				} else {
					if (val instanceof final Number n) {
						val = coerceNumber(n, targetType);
					}
					if (meta.enumConstants != null && meta.enumConstants[idx] != null && val != null)
						val = Meta.resolveEnum(meta.enumConstants[idx], val);
					objs[idx] = val;
				}
			}
		}
		return (T) meta.factory.create(objs, prims);
	}

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
				default -> {
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
						if      (targetType == Integer.class || targetType == int.class)    val = (int) primBits;
						else if (targetType == Short.class   || targetType == short.class)  val = (short) primBits;
						else if (targetType == Byte.class    || targetType == byte.class)   val = (byte) primBits;
						else if (targetType == Double.class  || targetType == double.class) val = (double) primBits;
						else if (targetType == Float.class   || targetType == float.class)  val = (float) primBits;
						else                                                                val = primBits;
					} else if (isDouble) {
						final var dVal = Double.longBitsToDouble(primBits);
						if      (targetType == Float.class   || targetType == float.class)  val = (float) dVal;
						else if (targetType == Integer.class || targetType == int.class)    val = (int) dVal;
						else if (targetType == Long.class    || targetType == long.class)   val = (long) dVal;
						else if (targetType == Short.class   || targetType == short.class)  val = (short) dVal;
						else if (targetType == Byte.class    || targetType == byte.class)   val = (byte) dVal;
						else                                                                val = dVal;
					} else if (val instanceof final Number n) {
						val = coerceNumber(n, targetType);
					}

					if (val instanceof final Map<?, ?> m && targetType != Object.class && targetType != Map.class) {
						val = fromMap((Map<String, Object>) m, targetType);
					}
					else if (val instanceof final Collection<?> c && (targetType.isArray() || Collection.class.isAssignableFrom(targetType))) {
						val = fromCollection(c, meta.components[targetIdx]);
					}

					if (meta.enumConstants != null && meta.enumConstants[targetIdx] != null && val != null) {
						val = Meta.resolveEnum(meta.enumConstants[targetIdx], val);
					}
					objs[targetIdx] = val;
				}
			}
		}
		return (T) meta.factory.create(objs, prims);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fromCollection(final Collection<?> source, final ObjectMeta.ComponentMeta comp) throws Throwable {
		if (source == null) return null;
		final var targetType = comp.type();
		var compClass = GernericsHandler.resolveClass(comp.valueType());
		if (targetType.isArray() && (compClass == null || compClass == Object.class || compClass == targetType)) {
			compClass = targetType.getComponentType();
		}
		if (compClass == null) compClass = Object.class;

		final var size = source.size();
		if (targetType.isArray()) {
			final var result = Array.newInstance(compClass, size);
			var i = 0;
			for (final var item : source) {
				var itemVal = item;
				if (item instanceof final Map<?, ?> m && compClass != Object.class && compClass != Map.class) {
					itemVal = fromMap((Map<String, Object>) m, compClass);
				} else if (itemVal instanceof final Number n) {
					itemVal = coerceNumber(n, compClass);
				} else if (compClass.isEnum() && itemVal != null) {
					itemVal = Meta.resolveEnum(compClass.getEnumConstants(), itemVal);
				}
				Array.set(result, i++, itemVal);
			}
			return (T) result;
		}
		final java.util.Collection<Object> result;
		if (Set.class.isAssignableFrom(targetType)) {
			result = new java.util.HashSet<>(size);
		} else {
			result = new java.util.ArrayList<>(size);
		}
		for (final var item : source) {
			var itemVal = item;
			if (item instanceof final Map<?, ?> m && compClass != Object.class && compClass != Map.class) {
				itemVal = fromMap((Map<String, Object>) m, compClass);
			} else if (itemVal instanceof final Number n) {
				itemVal = coerceNumber(n, compClass);
			} else if (compClass.isEnum() && itemVal != null) {
				itemVal = Meta.resolveEnum(compClass.getEnumConstants(), itemVal);
			}
			result.add(itemVal);
		}
		return (T) result;
	}

	private static Object coerceNumber(final Number n, final Class<?> targetType) {
		if (targetType == Integer.class || targetType == int.class)    return n.intValue();
		if (targetType == Long.class    || targetType == long.class)   return n.longValue();
		if (targetType == Double.class  || targetType == double.class) return n.doubleValue();
		if (targetType == Float.class   || targetType == float.class)  return n.floatValue();
		if (targetType == Short.class   || targetType == short.class)  return n.shortValue();
		if (targetType == Byte.class    || targetType == byte.class)   return n.byteValue();
		return n;
	}
}