package org.suche.protobuf;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.suche.json.ConstructorGenerator;
import org.suche.json.ConstructorGenerator.ObjectArrayFactory;
import org.suche.json.GernericsHandler;

public class ObjectMeta<T extends Record> {
	static final int T_STRING     =  0;
	static final int T_FLOAT_ARR  =  1;
	static final int T_DOUBLE_ARR =  2;
	static final int T_INT_ARR    =  3;
	static final int T_LONG_ARR   =  4;
	static final int T_INT        =  5;
	static final int T_LONG       =  6;
	static final int T_FLOAT      =  7;
	static final int T_DOUBLE     =  8;
	static final int T_RECORD     =  9;
	static final int T_OBJECT     = 10;

	private static int classIndex(final Class<?> ec) {
		if (ec == String  .class) return T_STRING;
		if (ec == float [].class) return T_FLOAT_ARR;
		if (ec == double[].class) return T_DOUBLE_ARR;
		if (ec == int   [].class) return T_INT_ARR;
		if (ec == long  [].class) return T_LONG_ARR;
		if (ec == int.class    || ec == Integer.class) return T_INT;
		if (ec == long.class   || ec == Long   .class) return T_LONG;
		if (ec == float.class  || ec == Float  .class) return T_FLOAT;
		if (ec == double.class || ec == Double .class) return T_DOUBLE;
		if (Record.class.isAssignableFrom(ec)) return T_RECORD;
		return T_OBJECT;
	}

	private static final Map<Class<? extends Record>, ObjectMeta<?>> cache = new ConcurrentHashMap<>();
	private final Map<Integer , FieldInfo > fieldMap = new HashMap<>();
	private final ObjectArrayFactory factory;
	private final Class<T> recordClass;

	public record FieldInfo(int cpos, String keyName, boolean isRepeated, Class<?> effCls, int classIndex) {}

	@SuppressWarnings("unchecked")
	public static <T extends Record> ObjectMeta<T> of(final Class<T> c) {
		return (ObjectMeta<T>)cache.computeIfAbsent(c, ObjectMeta::new);
	}

	public int fieldSize() { return fieldMap.size(); }

	public FieldInfo field(final int fieldId) { return fieldMap.get(fieldId); }

	void skip(final int fieldId) {
		if(fieldMap.containsKey(fieldId)) return;
		System.err.println("Unsupported field "+fieldId + " for class " + recordClass.getCanonicalName());
		fieldMap.put(fieldId, new FieldInfo(-1, "", false, Void.class, T_OBJECT));
	}

	private static IllegalStateException dupplicate(final int id, final String o, final String n) {
		throw new IllegalStateException("Duplcate ID "+id+" for field "+o+" and "+n);
	}

	private static IllegalStateException invalidId(final int id, final String o) {
		throw new IllegalStateException("Invalid Protobuf Field-ID " + id + " for record component '" + o + "'");
	}

	private ObjectMeta(final Class<T> cls) {
		this.recordClass = cls;
		var idx = 0;
		for (final var component : recordClass.getRecordComponents()) {
			final var cpos = idx;
			idx++;
			final var field      = component.getAnnotation(Fld.class) instanceof final Fld f ? f.value() : idx;
			final var name       = component.getName();
			if (field <= 0 || (field >= 19000 && field <= 19999) || field > 536870911) throw invalidId(field, name);
			final var generic    = component.getGenericType();
			final var raw        = GernericsHandler.resolveClass(generic);
			final var isRepeated = Collection.class.isAssignableFrom(raw) || raw.isArray();
			final var ec         = isRepeated ? GernericsHandler.resolveClass(generic) : raw;
			final var classIndex = classIndex(ec);
			if(T_OBJECT == classIndex) System.err.println("Unsupported type " + ec.getCanonicalName() + " for field:" + field + ":"+name+" for class " + recordClass.getCanonicalName());
			if(fieldMap.put(field, new FieldInfo(cpos, name, isRepeated, ec, classIndex)) instanceof final FieldInfo old)
				throw dupplicate(field, old.keyName, name);
		}
		try { factory    = ConstructorGenerator.generate(recordClass); }
		catch(final Exception e) { throw new RuntimeException(e); }
	}

	public Object create(final Object[] objects, final long[] primitives) { return factory.create(objects, primitives); }
}