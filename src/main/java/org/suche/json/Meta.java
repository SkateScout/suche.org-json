package org.suche.json;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.suche.json.MetaConfig.Filter;

final class Meta {
	private static final Lookup       lookup            = MethodHandles.lookup();
	private static final MethodType   MT_FUNC_INT       = MethodType.methodType(ToIntFunction.class);
	private static final MethodType   MT_FUNC_LONG      = MethodType.methodType(ToLongFunction.class);
	private static final MethodType   MT_FUNC_DOUBLE    = MethodType.methodType(ToDoubleFunction.class);
	private static final MethodType   MT_FUNC_BOOL      = MethodType.methodType(Predicate.class);
	private static final MethodType   MT_S_APPLY_INT    = MethodType.methodType(int.class,     Object.class);
	private static final MethodType   MT_S_APPLY_LONG   = MethodType.methodType(long.class,    Object.class);
	private static final MethodType   MT_S_APPLY_DOUBLE = MethodType.methodType(double.class,  Object.class);
	private static final MethodType   MT_S_APPLY_BOOL   = MethodType.methodType(boolean.class, Object.class);
	private static final MethodType   MT_FUNC           = MethodType.methodType(Function.class);
	private static final MethodType   MT_S_APPLY        = MethodType.methodType(Object.class, Object.class);
	private static final MethodType   ENUM_MFILTER      = MethodType.methodType(Object.class, Object[].class, Object[].class);
	private static final MethodType   MT_SUPPLIER       = MethodType.methodType(Supplier.class);
	private static final MethodType   MT_OBJECT         = MethodType.methodType(Object.class);

	private static final MethodHandle constructEnumObj;
	private static final MethodHandle enumFilter;

	static {
		try {  constructEnumObj = lookup.findStatic(Meta.class, "constructEnumObj", ENUM_MFILTER); } catch(final Throwable t) { throw new RuntimeException(t); }
		try {  enumFilter       = lookup.findStatic(Meta.class, "resolveEnum", MethodType.methodType(Object.class, Object[].class, Object.class)); } catch(final Throwable t) { throw new RuntimeException(t); }
	}

	static KeyValueObject createFastFieldGetter(final byte[] key, final Field f, final Filter filter) throws Throwable {
		try { f.setAccessible(true); } catch (final Exception ignored) {}
		final var handle = lookup.unreflectGetter(f);
		final var ret    = f.getType();
		if (filter != null) {
			final var mh = handle.asType(MT_S_APPLY);
			return new KeyValueObject(key, 0, obj -> {
				try { return filter.apply(mh.invokeExact(obj)); }
				catch (final Throwable x) { throw new RuntimeException(x); }
			}, null, null, null, null);
		}
		if (ret == int.class || ret == short.class || ret == byte.class) return new KeyValueObject(key, 1, null, obj -> { try { return (int) handle.asType(MT_S_APPLY_INT).invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null, null);
		if (ret == long.class                                          ) return new KeyValueObject(key, 2, null, null, obj -> { try { return (long) handle.asType(MT_S_APPLY_LONG).invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null);
		if (ret == double.class || ret == float.class                  ) return new KeyValueObject(key, 3, null, null, null, obj -> { try { return (double) handle.asType(MT_S_APPLY_DOUBLE).invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null);
		if (ret == boolean.class                                       ) return new KeyValueObject(key, 4, null, null, null, null, obj -> { try { return (boolean) handle.asType(MT_S_APPLY_BOOL).invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } });
		final var mh = handle.asType(MT_S_APPLY);
		return new KeyValueObject(key, 0, obj -> { try { return mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null, null, null);
	}

	@ SuppressWarnings("unchecked")
	static KeyValueObject createFastGetter(final byte[] key, final Method m, final Filter filter) throws Throwable {
		try { m.setAccessible(true); } catch (final Exception ignored) {}
		final var handle = lookup.unreflect(m);
		final var c      = m.getDeclaringClass();
		final var ret    = m.getReturnType();
		if (filter != null) {
			final var mh = handle.asType(MT_S_APPLY);
			return new KeyValueObject(key, 0, obj -> {
				try { return filter.apply(mh.invokeExact(obj)); }
				catch (final Throwable x) { throw new RuntimeException(x); }
			}, null, null, null, null);
		}
		if (ret == int.class || ret == short.class || ret == byte.class) {
			try {
				final var site = LambdaMetafactory.metafactory(lookup, "applyAsInt", MT_FUNC_INT, MT_S_APPLY_INT, handle, MethodType.methodType(ret, c));
				return new KeyValueObject(key, 1, null, (ToIntFunction<Object>) site.getTarget().invokeExact(), null, null, null);
			} catch (final Throwable e) {
				final var mh = handle.asType(MT_S_APPLY_INT);
				return new KeyValueObject(key, 1, null, obj -> { try { return (int) mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null, null);
			}
		}
		if (ret == long.class) {
			try {
				final var site = LambdaMetafactory.metafactory(lookup, "applyAsLong", MT_FUNC_LONG, MT_S_APPLY_LONG, handle, MethodType.methodType(long.class, c));
				return new KeyValueObject(key, 2, null, null, (ToLongFunction<Object>) site.getTarget().invokeExact(), null, null);
			} catch (final Throwable e) {
				final var mh = handle.asType(MT_S_APPLY_LONG);
				return new KeyValueObject(key, 2, null, null, obj -> { try { return (long) mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null);
			}
		}
		if (ret == double.class || ret == float.class) {
			try {
				final var site = LambdaMetafactory.metafactory(lookup, "applyAsDouble", MT_FUNC_DOUBLE, MT_S_APPLY_DOUBLE, handle, MethodType.methodType(ret, c));
				return new KeyValueObject(key, 3, null, null, null, (ToDoubleFunction<Object>) site.getTarget().invokeExact(), null);
			} catch (final Throwable e) {
				final var mh = handle.asType(MT_S_APPLY_DOUBLE);
				return new KeyValueObject(key, 3, null, null, null, obj -> { try { return (double) mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null);
			}
		}
		if (ret == boolean.class) {
			try {
				final var site = LambdaMetafactory.metafactory(lookup, "test", MT_FUNC_BOOL, MT_S_APPLY_BOOL, handle, MethodType.methodType(boolean.class, c));
				return new KeyValueObject(key, 4, null, null, null, null, (Predicate<Object>) site.getTarget().invokeExact());
			} catch (final Throwable e) {
				final var mh = handle.asType(MT_S_APPLY_BOOL);
				return new KeyValueObject(key, 4, null, null, null, null, obj -> { try { return (boolean) mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } });
			}
		}
		try {
			final var site = LambdaMetafactory.metafactory(lookup, "apply", MT_FUNC, MT_S_APPLY, handle, MethodType.methodType(ret, c));
			return new KeyValueObject(key, 0, (Function<Object, Object>) site.getTarget().invokeExact(), null, null, null, null);
		} catch (final Throwable e) {
			final var mh = handle.asType(MT_S_APPLY);
			return new KeyValueObject(key, 0, obj -> { try { return mh.invokeExact(obj); } catch (final Throwable x) { throw new RuntimeException(x); } }, null, null, null, null);
		}
	}

	static Object resolveEnum(final Object[] constants, final Object value) {
		return switch(value) {
		case final String      s -> {
			for (final Object e : constants) if (((Enum<?>) e).name().equals(s)) yield e;
			System.err.println("Value : "+value+" is not an valid enum name");
			yield null;
		}
		case final Enum<?>     e -> e;
		case final Integer     i -> (i >= 0 && i < constants.length ? constants[i] : null);
		case null,default        -> null;
		};
	}

	static Object constructEnumObj(final Object[] enums, final Object[] classAndValue) { return resolveEnum(enums, classAndValue[1]); }

	static MethodHandle ofEnum(final Class<?> c) { return MethodHandles.insertArguments(constructEnumObj, 0, (Object)c.getEnumConstants()); }

	static MethodHandle newFilter(final Class<?> c) {
		if (c.isEnum())
			try {
				return MethodHandles.insertArguments(Meta.enumFilter, 0, (Object) c.getEnumConstants()).asType(MethodType.methodType(c, Object.class));
			} catch (final Throwable e) { throw new RuntimeException(e); }
		return null;
	}


	@ SuppressWarnings("unchecked")
	static Supplier<Object> asSupplier(final Class<?> type, final MethodHandle ctorHandle) {
		try {
			return (Supplier<Object>) LambdaMetafactory.metafactory(lookup, "get", MT_SUPPLIER, MT_OBJECT, ctorHandle, MethodType.methodType(type)).getTarget().invokeExact();
		} catch (final Throwable e) {
			return () -> { try { return ctorHandle.asType(MethodType.methodType(Object.class)).invokeExact(); } catch (final Throwable t) { throw new RuntimeException(t); } };
		}
	}
}