package org.suche.json;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.suche.json.MetaConfig.Filter;
import org.suche.json.MetaConfig.NameFilter;

record KeyValueObject(
		byte[] jsonFirstKeyBytes,
		byte[] jsonNextKeyBytes,
		int    type, // 0=Object, 1=Int, 2=Long, 3=Double, 4=Boolean
		java.util.function.Function        <Object, Object> objGetter,
		java.util.function.ToIntFunction   <Object> intGetter,
		java.util.function.ToLongFunction  <Object> longGetter,
		java.util.function.ToDoubleFunction<Object> doubleGetter,
		java.util.function.Predicate       <Object> boolGetter
		) {

	byte[] jsonKeyBytes(final boolean second) { return second ? jsonNextKeyBytes : jsonFirstKeyBytes; }

	public KeyValueObject(final byte[] jsonFirstKeyBytes, final int type,
			final UnaryOperator<Object> objGetter, final java.util.function.ToIntFunction<Object> intGetter,
			final java.util.function.ToLongFunction<Object> longGetter,
			final java.util.function.ToDoubleFunction<Object> doubleGetter, final java.util.function.Predicate<Object> boolGetter) {
		this(jsonFirstKeyBytes, null, type, objGetter, intGetter, longGetter, doubleGetter, boolGetter);
	}

	KeyValueObject { jsonNextKeyBytes = commaPrefix(jsonFirstKeyBytes); }

	private static final Logger     LOG               = Logger.getLogger(KeyValueObject.class.getCanonicalName());
	static final byte[]     CLASS_NAME_FIRST        = "__class__:".getBytes();
	static final byte[]     CLASS_NAME_NEXT        = "__class__:".getBytes();

	static byte[] commaPrefix(final byte[] b) { // KeyValueObject.kommaPrefix
		final var r = new byte[b.length+1];
		System.arraycopy(b, 0, r, 1, b.length);
		r[0]=',';
		return r;
	}

	static final KeyValueObject[] NULL = {};

	static int addComponent(final KeyValueObject[] result, int cnt, final RecordComponent comp, final BiFunction<String, RecordComponent, Object> mapComponent) {
		Filter filter = null;
		var name = comp.getName();
		if(mapComponent != null)
			try {
				switch(mapComponent.apply(name,comp)) {
				case null                                 -> { /* no renaming */ }
				case final String     t                   -> name   = t;
				case final Filter     t                   -> filter = t;
				case NameFilter(final var n, final var f) -> { name = n;  filter = f; }
				default                                   -> throw new IllegalStateException();
				}
			} catch(final Exception t) {
				if(t == MetaConfig.SKIP_FIELD) return cnt;
				if(t instanceof final RuntimeException e) throw e;
				throw new IllegalStateException(t);
			}
		try {
			result[cnt++] = Meta.createFastGetter(JSONString.buildJsonKey(name, true), comp.getAccessor(), filter);
		} catch (final Throwable e) {
			LOG.log(Level.SEVERE, e, ()->"Failed to access record component: " + comp.getDeclaringRecord().getCanonicalName() + "." + comp.getName() + "=> " + e.getMessage());
		}
		return cnt;
	}

	static KeyValueObject[] ofRecord(final Class<? extends Record> clazz, final MetaConfig cfg) {
		final var components = clazz.getRecordComponents();
		final var len = components.length;
		final var result = new KeyValueObject[len];
		var cnt = 0;
		final var mapComponent = cfg.mapComponent();
		for (var i = 0; i < len; i++) cnt = addComponent(result, cnt, components[i], mapComponent);
		return(cnt < len ? Arrays.copyOf(result, cnt) : result);
	}

	private static int registerComplexMethod(final KeyValueObject[] result, int cnt, final Method m, String name, final Set<String> seenKeys, final BiFunction<String, Method, Object> mapMethod) {
		Filter filter = null;
		if(mapMethod != null)
			try {
				switch(mapMethod.apply(name,m)) {
				case null                                 -> { /* no renaming */ }
				case final String     t                   -> name   = t;
				case final Filter     t                   -> filter = t;
				case NameFilter(final var n, final var f) -> { name = n;  filter = f; }
				default                                   -> throw new IllegalStateException();
				}
			} catch(final Exception t) {
				if(t == MetaConfig.SKIP_FIELD) return cnt;
				if(t instanceof final RuntimeException e) throw e;
				throw new IllegalStateException(t);
			}

		if (seenKeys.add(name))
			try {
				result[cnt++] = Meta.createFastGetter(JSONString.buildJsonKey(name, true), m, filter);
			} catch (final Throwable e) {
				LOG.log(Level.WARNING, e, ()->"Failed to build fast getter for: " + m.getDeclaringClass()+"." +m.getName());
			}
		return cnt;
	}

	private static int registerComplexMethods(final KeyValueObject[] result, int cnt, final Method[] methods, final Set<String> seenKeys, final BiFunction<String, Method, Object> mapMethod) {
		for (final var m : methods) {
			if (m.getParameterCount() == 0
					&& m.getDeclaringClass() != Object.class
					&& !Modifier.isStatic(m.getModifiers())) {
				final var methodName = m.getName();
				var name = methodName;
				if      (name.startsWith("get") && name.length() > 3 &&  m.getReturnType() != void.class)                                           name = Character.toLowerCase(name.charAt(3)) + name.substring(4);
				else if (name.startsWith("is")  && name.length() > 2 && (m.getReturnType() == boolean.class || m.getReturnType() == Boolean.class)) name = Character.toLowerCase(name.charAt(2)) + name.substring(3);
				else continue;
				cnt += registerComplexMethod(result, cnt, m, name, seenKeys, mapMethod);
			}
		}
		return cnt;
	}

	private static int registerComplexfield(final KeyValueObject[] result, int cnt, final Field f, final Set<String> seenKeys, final BiFunction<String, Field , Object> mapField)  {
		Filter filter = null;
		var name = f.getName();
		if(mapField != null)
			try {
				switch(mapField.apply(name,f)) {
				case null                                  -> { /* no renaming */ }
				case final String     t                    -> name   = t;
				case final Filter     t                    -> filter = t;
				case NameFilter(final var n, final var fi) -> { name = n;  filter = fi; }
				default                                    -> throw new IllegalStateException();
				}
			} catch(final Exception t) {
				if(t == MetaConfig.SKIP_FIELD) return cnt;
				if(t instanceof final RuntimeException e) throw e;
				throw new IllegalStateException(t);
			}
		if(seenKeys.add(name)) // Prefer getter
			try {
				result[cnt++] = Meta.createFastFieldGetter(JSONString.buildJsonKey(name, true), f, filter);
			} catch (final Throwable e) {
				LOG.log(Level.WARNING, e, () -> "Failed to build fast field getter for: {}" + f.getDeclaringClass().getCanonicalName() + "." + f.getName());
			}
		return cnt;
	}

	private static int registerComplexfields(final KeyValueObject[] result, int cnt, final Field[] fields, final Set<String> seenKeys, final BiFunction<String, Field , Object> mapField)  {
		for (final var f : fields) {
			if (!Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers())) {
				cnt = registerComplexfield(result, cnt, f, seenKeys, mapField);
			}
		}
		return cnt;
	}

	static KeyValueObject[]  registerComplex(Class<?> c, final MetaConfig cfg) {
		while(c.isArray()) c = c.componentType();
		if(c.isRecord()) return ofRecord(c.asSubclass(Record.class), cfg);
		final var methods = c.getMethods();
		final var fields  = c.getDeclaredFields();
		final var result = new KeyValueObject[methods.length + fields.length];
		final var seenKeys = new HashSet<String>();
		final var mapField  = cfg.mapField();
		var cnt = 0;
		cnt = registerComplexMethods(result, cnt, methods, seenKeys, cfg.mapMethod());
		cnt = registerComplexfields(result, cnt, fields, seenKeys, mapField);
		return(cnt < result.length ? Arrays.copyOf(result, cnt) : result);
	}
}