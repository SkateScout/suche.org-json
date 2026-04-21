package org.suche.json;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.suche.json.MetaConfig.Filter;
import org.suche.json.MetaConfig.NameFilter;

record KeyValueObject(
		byte[] jsonKeyBytes,
		int type, // 0=Object, 1=Int, 2=Long, 3=Double, 4=Boolean
		java.util.function.Function        <Object, Object> objGetter,
		java.util.function.ToIntFunction   <Object> intGetter,
		java.util.function.ToLongFunction  <Object> longGetter,
		java.util.function.ToDoubleFunction<Object> doubleGetter,
		java.util.function.Predicate       <Object> boolGetter
		) {
	private static final Logger     LOG               = Logger.getLogger(KeyValueObject.class.getCanonicalName());
	static final byte[]     CLASS_NAME        = JSONString.buildJsonKey("__class__", true);
	static final KeyValueObject[] NULL = {};

	static KeyValueObject[] ofRecord(final Class<? extends Record> clazz, final MetaConfig cfg) {
		final var components = clazz.getRecordComponents();
		final var len = components.length;
		final var result = new KeyValueObject[len];
		var cnt = 0;
		final var mapComponent = cfg.mapComponent();
		for (var i = 0; i < len; i++) {
			final var comp = components[i];
				Filter filter = null;
				var name = comp.getName();
			if(mapComponent != null)
				try {
					switch(mapComponent.apply(name,comp)) {
				case null                -> { }
				case final String     t -> name   = t;
				case final Filter     t -> filter = t;
				case final NameFilter t -> { name = t.name();  filter = t.filter(); }
				default             -> { throw new IllegalStateException(); }
				}
				} catch(final Throwable t) {
					if(t == MetaConfig.SKIP_FIELD) continue;
					if(t instanceof final RuntimeException e) throw e;
					throw new RuntimeException(t);
				}
			try {
				result[cnt++] = Meta.createFastGetter(JSONString.buildJsonKey(name, true), comp.getAccessor(), filter);
			} catch (final Throwable e) { LOG.log(Level.SEVERE, "Failed to access record component: " + clazz.getCanonicalName() + "." + comp.getName() + "=> " + e.getMessage(), e); }
		}
		return(cnt < len ? Arrays.copyOf(result, cnt) : result);
	}

	static KeyValueObject[]  registerComplex(Class<?> c, final MetaConfig cfg) {
		while(c.isArray()) c = c.componentType();
		if(c.isRecord()) return ofRecord(c.asSubclass(Record.class), cfg);
		final var methods = c.getMethods();
		final var fields  = c.getDeclaredFields();
		final var result = new KeyValueObject[methods.length + fields.length];
		var cnt = 0;
		final var seenKeys = new HashSet<String>();
		final var mapField  = cfg.mapField();
		final var mapMethod = cfg.mapMethod();
		for (final var m : methods) {
			if (	   m.getParameterCount() != 0
					|| m.getDeclaringClass() == Object.class
					|| Modifier.isStatic(m.getModifiers())) continue;
			Filter filter = null;
			var name = m.getName();
			if      (name.startsWith("get") && name.length() > 3 &&  m.getReturnType() != void.class)                                           name = Character.toLowerCase(name.charAt(3)) + name.substring(4);
			else if (name.startsWith("is")  && name.length() > 2 && (m.getReturnType() == boolean.class || m.getReturnType() == Boolean.class)) name = Character.toLowerCase(name.charAt(2)) + name.substring(3);
			else continue;


			if(mapMethod != null)
				try {
					switch(mapMethod.apply(name,m)) {
					case null                -> { }
					case final String     t -> name   = t;
					case final Filter     t -> filter = t;
					case final NameFilter t -> { name = t.name();  filter = t.filter(); }
					default             -> { throw new IllegalStateException(); }
					}
				} catch(final Throwable t) {
					if(t == MetaConfig.SKIP_FIELD) continue;
					if(t instanceof final RuntimeException e) throw e;
					throw new RuntimeException(t);
				}

			if (!seenKeys.add(name)) continue;
			try {
				result[cnt++] = Meta.createFastGetter(JSONString.buildJsonKey(name, true), m, filter);
			} catch (final Throwable e) {
				LOG.log(Level.WARNING, "Failed to build fast getter for: " + c.getName() + "." + name, e);
			}
		}
		for (final var f : fields) {
			if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) continue;
			Filter filter = null;
			var name = f.getName();
			if(mapField != null)
				try {
					switch(mapField.apply(name,f)) {
					case null                -> { }
					case final String     t -> name   = t;
					case final Filter     t -> filter = t;
					case final NameFilter t -> { name = t.name();  filter = t.filter(); }
					default             -> { throw new IllegalStateException(); }
					}
				} catch(final Throwable t) {
					if(t == MetaConfig.SKIP_FIELD) continue;
					if(t instanceof final RuntimeException e) throw e;
					throw new RuntimeException(t);
				}
			if(!seenKeys.add(name)) continue;	// Prever getter
			try {
				result[cnt++] = Meta.createFastFieldGetter(JSONString.buildJsonKey(name, true), f, filter);
			} catch (final Throwable e) {
				LOG.log(Level.WARNING, "Failed to build fast field getter for: " + c.getName() + "." + name, e);
			}
		}
		return(cnt < result.length ? Arrays.copyOf(result, cnt) : result);
	}
}