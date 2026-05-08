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
				else if (val instanceof final Collection<?> c && meta.components[idx].type().isArray()) val = fromCollection(c, meta.components[idx].type());
				else if (val instanceof final Boolean b) {
					objs[idx] = b;
					if(prims != null) prims[idx] = b ? 1 : 0;
				}

				// Assign primitive values directly to avoid boxing overhead in the target constructor
				if (meta.components[idx].type().isPrimitive()) {
					var primVal = 0L;
					if (val instanceof final Number n) {
						if      (meta.components[idx].type() == double.class) primVal = Double.doubleToRawLongBits(n.doubleValue());
						else if (meta.components[idx].type() == float.class ) primVal = Float.floatToRawIntBits(n.floatValue());
						else                                                  primVal = n.longValue();
					}
					if (prims == null) JsonEngine.illegalStateException("Missing prims");
					else prims[idx] = primVal;
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