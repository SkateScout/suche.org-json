package org.suche.json;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class GernericsHandler {

	static Type resolveActualType(final Type genericType, final Type contextType) {
		if (genericType == null) return Object.class;
		return resolveType(genericType, buildContextMap(contextType));
	}

	static Type extractValueType(Type genericType, final Type contextType) {
		if (genericType == null) return Object.class;
		final var typeMap = buildContextMap(contextType);
		genericType = resolveType(genericType, typeMap);
		if (genericType instanceof final GenericArrayType gat) return resolveType(gat.getGenericComponentType(), typeMap);
		final Class<?> rawType = resolveClass(genericType);
		if (rawType.isArray()) return rawType.getComponentType();

		final Class<?> targetInterface;
		final int targetIndex;
		if (Map.class.isAssignableFrom(rawType)) {
			targetInterface = Map.class;
			targetIndex = 1;
		} else if (Collection.class.isAssignableFrom(rawType)) {
			targetInterface = Collection.class;
			targetIndex = 0;
		} else return genericType;

		final var targetType = findExactSuperType(genericType, targetInterface, typeMap);
		if (targetType instanceof final ParameterizedType pt) return resolveType(pt.getActualTypeArguments()[targetIndex], typeMap);
		return Object.class;
	}

	// Maps TypeVariables directly to prevent JVM identity loss and endless loops
	private static Map<TypeVariable<?>, Type> buildContextMap(final Type contextType) {
		final var map = new HashMap<TypeVariable<?>, Type>();
		if (contextType instanceof final ParameterizedType pt && pt.getRawType() instanceof final Class<?> raw) {
			final var vars = raw.getTypeParameters();
			final var args = pt.getActualTypeArguments();
			for (var i = 0; i < vars.length && i < args.length; i++) map.put(vars[i], args[i]); // <- Using TypeVariable instead of String
		}
		return map;
	}

	private static Type resolveType(Type type, final Map<TypeVariable<?>, Type> map) {
		while (type instanceof final TypeVariable<?> tv && map.containsKey(tv)) type = map.get(tv);
		if (type instanceof final ParameterizedType pt) return resolveParameterizedType(pt, map);
		if (type instanceof final GenericArrayType gat) {
			final var comp = resolveType(gat.getGenericComponentType(), map);
			if (comp instanceof final Class<?> c) return c.arrayType();
			return gat;
		}
		return type;
	}

	private static Type fromParameterizedType(final ParameterizedType pt, final Class<?> targetClass, final Map<TypeVariable<?>, Type> typeMap) {
		final var raw = (Class<?>) pt.getRawType();
		if (raw == targetClass) return resolveParameterizedType(pt, typeMap);
		final var vars = raw.getTypeParameters();
		final var args = pt.getActualTypeArguments();
		final var nextMap = new HashMap<>(typeMap);
		for (var i = 0; i < vars.length; i++) {
			var arg = args[i];
			while (arg instanceof final TypeVariable<?> tv && nextMap.containsKey(tv)) arg = nextMap.get(tv);
			nextMap.put(vars[i], arg);
		}

		for (final var intf : raw.getGenericInterfaces())
			if (findExactSuperType(intf, targetClass, nextMap) instanceof final Type found) return found;
		if (raw.getGenericSuperclass() instanceof final Type sup) return findExactSuperType(sup, targetClass, nextMap);
		return null;
	}

	private static Type findExactSuperType(final Type currentType, final Class<?> targetClass, final Map<TypeVariable<?>, Type> typeMap) {
		if (currentType instanceof final Class<?> c) {
			if (c == targetClass) return c;
			for (final var intf : c.getGenericInterfaces()) if (findExactSuperType(intf, targetClass, new HashMap<>(typeMap)) instanceof final Type found) return found;
			if (c.getGenericSuperclass() instanceof final Type sup) return findExactSuperType(sup, targetClass, new HashMap<>(typeMap));
			return null;
		}

		if (currentType instanceof final ParameterizedType pt) return fromParameterizedType(pt, targetClass, typeMap);
		return null;
	}

	private static ParameterizedType resolveParameterizedType(final ParameterizedType pt, final Map<TypeVariable<?>, Type> typeMap) {
		final var originalArgs = pt.getActualTypeArguments();
		final var resolvedArgs = new Type[originalArgs.length];
		for (var i = 0; i < originalArgs.length; i++) {
			var arg = originalArgs[i];
			while (arg instanceof final TypeVariable<?> tv && typeMap.containsKey(tv)) arg = typeMap.get(tv);
			resolvedArgs[i] = arg;
		}
		return new ResolvedParameterizedType(pt, resolvedArgs);
	}

	private record ResolvedParameterizedType(ParameterizedType original, Type[] resolvedArgs) implements ParameterizedType {
		@Override public Type[] getActualTypeArguments() { return resolvedArgs; }
		@Override public Type   getRawType            () { return original.getRawType(); }
		@Override public Type   getOwnerType          () { return original.getOwnerType(); }
	}

	public static Class<?> resolveClass(Type t) {
		while(true) {
			switch(t) {
			case final Class<?> c           -> { return c; }
			case final ParameterizedType pt -> t = pt.getRawType();
			case final WildcardType      wt when wt.getUpperBounds().length > 0 -> t = wt.getUpperBounds()[0];
			case final TypeVariable<?>   tv when tv.getBounds     ().length > 0 -> t = tv.getBounds     ()[0];
			case final GenericArrayType  gat                                    -> { return resolveClass(gat.getGenericComponentType()).arrayType(); }
			default                          -> { return Object.class; }
			}
		}
	}
}