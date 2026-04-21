package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;

import org.suche.json.MetaPool.CtxArrays;

interface ObjBooleanConsumer<T> { void accept(T t, boolean value); }

final class PropSetter {
	final BiConsumer        <Object, Object> objSetter   ;
	final ObjIntConsumer    <Object        > intSetter   ;
	final ObjLongConsumer   <Object        > longSetter  ;
	final ObjDoubleConsumer <Object        > doubleSetter;
	final ObjBooleanConsumer<Object        > boolSetter  ;

	PropSetter(final BiConsumer<Object, Object> o, final ObjIntConsumer<Object> i, final java.util.function.ObjLongConsumer<Object> l, final ObjDoubleConsumer<Object> d, final ObjBooleanConsumer<Object> b) {
		this.objSetter = o; this.intSetter = i; this.longSetter = l; this.doubleSetter = d; this.boolSetter = b;
	}
}

public final class ObjectMeta {
	private static final int                  TYPE_POJO     = 0;
	static         final int                  TYPE_RECORD   = 1;
	private static final int                  TYPE_MAP      = 2;
	private static final int                  TYPE_DEFECT   = 3;
	private static final int                  TYPE_SEALED   = 4;
	private static final int                  TYPE_MIXED    = 5;
	private static final MethodHandles.Lookup LOOKUP        = MethodHandles.lookup();
	static         final ObjectMeta           DEFECT_FIRST  = new ObjectMeta();
	static         final ObjectMeta           DEFECT        = new ObjectMeta();
	static         final RuntimeException     E_DEFEKT2     = new RuntimeException("DEFEKT.2", null, false, false) { };
	private static final RuntimeException     E_TYPE1       = new RuntimeException("TYPE.1", null, false, false) { };
	private static final RuntimeException     E_TYPE2       = new RuntimeException("TYPE.2", null, false, false) { };
	private static final RuntimeException     E_TYPE3       = new RuntimeException("TYPE.2", null, false, false) { };
	static         final ObjectMeta           NULL          = new ObjectMeta(null);
	static         final ObjectMeta           GENERIC_MAP   = new ObjectMeta(Object.class);
	private static final ComponentMeta[]      ENUM_COMPS    =  { new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class), new ComponentMeta("value", String.class) };
	private static final Class<?>     []      ENUM_TYPES    =  { String.class, String.class };
	private static final FastKeyTable         ENUM_KEYS     = FastKeyTable.build(ENUM_COMPS);

	final int metaType;
	private final String className;
	private final boolean failOnUnknown;
	final Supplier<Object> pojoStart;
	final PropSetter[] pojoSetters;
	final ConstructorGenerator.ObjectArrayFactory factory;
	final int          ctorParamCount;
	private final FastKeyTable keys          ;
	private final Class<?>[]   types         ;
	private final Class<?>     baseType      ;
	final Class<?>[]   permitted     ;
	final Object[][]   enumConstants ;

	record ComponentMeta(String name, Class<?> type) {
		public ComponentMeta {
			if(name == null) throw new IllegalStateException("Missing name");
			if(type == null) throw new IllegalStateException("Missing type");
		}
	}

	final ComponentMeta[] components;

	record FastKeyTable(String[] keys, int[] indices, int mask) {
		static FastKeyTable build(final ComponentMeta[] components) {
			if (components == null || components.length == 0) return new FastKeyTable(new String[0], new int[0], 0);
			var cap = 16;
			while (cap < components.length * 3) cap <<= 1;
			final var keys = new String[cap];
			final var indices = new int[cap];
			Arrays.fill(indices, -1);
			final var mask = cap - 1;
			for (var i = 0; i < components.length; i++) {
				final var key = components[i].name();
				var idx = key.hashCode() & mask;
				while (keys[idx] != null) idx = (idx + 1) & mask;
				keys[idx] = key;
				indices[idx] = i;
			}
			return new FastKeyTable(keys, indices, mask);
		}
		int get(final String key) {
			if (mask == 0) return -1;
			var idx = key.hashCode() & mask;
			while (keys[idx] != null) {
				if (keys[idx] == key || keys[idx].equals(key)) return indices[idx];
				idx = (idx + 1) & mask;
			}
			return -1;
		}
	}

	static final class ArrayBuilder {
		Object[] buf;
		int      cnt;
		String   currentKey;
		ArrayBuilder(final Object[] buf) { this.buf = buf; this.cnt = 0; }
	}

	static final record Prop(String name, boolean isField, Class<?> type, int ctorIdx, MethodHandle setterHandle) { }

	private static final int MOD_FINAL_OR_STATIC = Modifier.STATIC | Modifier.FINAL;

	private static Object[][] buildEnumConstants(final Class<?>[] types) {
		if (types == null) return null;
		var hasEnums = false;
		final var enums = new Object[types.length][];
		for (var i = 0; i < types.length; i++) {
			if (types[i] != null && types[i].isEnum()) {
				enums[i] = types[i].getEnumConstants();
				hasEnums = true;
			}
		}
		return hasEnums ? enums : null;
	}

	@SuppressWarnings("unchecked")
	static ObjectMeta ofPojo(final InternalEngine engine, final Class<?> c) {
		try {
			final var ctors = c.getDeclaredConstructors();
			Constructor<?> bestCtor = null;
			for (final var ctor : ctors) {
				if (ctor.getParameterCount() == 0) { bestCtor = ctor; break; }
				if (bestCtor == null || ctor.getParameterCount() > bestCtor.getParameterCount()) bestCtor = ctor;
			}
			if (bestCtor == null) {
				System.err.println("ObjectMeta.of("+c.getCanonicalName()+") => No Constructor found");
				return DEFECT_FIRST;
			}
			if (!bestCtor.canAccess(null)) bestCtor.setAccessible(true);

			final var params = bestCtor.getParameters();
			final var props  = new LinkedHashMap<String, Prop>();

			for (var i = 0; i < params.length; i++) {
				final var p = params[i];
				props.put(p.getName(), new Prop(p.getName(), false, p.getType(), i, null));
			}

			for (final var m : c.getMethods()) {
				if(m.getParameterCount() != 1 || m.getName().length() < 4 || !m.getName().startsWith("set")) continue;
				final var name = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
				if(props.containsKey(name)) continue;
				m.setAccessible(true);
				props.put(name, new Prop(name, false, m.getParameterTypes()[0], -1, LOOKUP.unreflect(m)));
			}

			for (final var f : c.getDeclaredFields()) {
				if (0 != (f.getModifiers() & MOD_FINAL_OR_STATIC)) continue;
				final var name = f.getName();
				if(props.containsKey(name)) continue;
				f.setAccessible(true);
				props.put(name, new Prop(name, true, f.getType(), -1, LOOKUP.unreflectSetter(f)));
			}

			final var totalProps   = props.size();
			final var finalComps   = new ComponentMeta[totalProps];
			final var finalTypes   = new Class<?>     [totalProps];
			final var finalSetters = new PropSetter   [totalProps];
			var setterCounter = params.length;

			for (final var entry : props.entrySet()) {
				final var name = entry.getKey();
				final var prop = entry.getValue();
				final var targetIdx = prop.ctorIdx != -1 ? prop.ctorIdx : setterCounter++;
				finalComps[targetIdx] = new ComponentMeta(name, prop.type);
				finalTypes[targetIdx] = prop.type;
				if (prop.setterHandle != null) finalSetters[targetIdx] = Meta.propSetter(c, prop.isField, prop.name, prop.setterHandle, prop.type, engine.getFilter(prop.type));
			}

			final var keys = FastKeyTable.build(finalComps);
			final var enumConstants = buildEnumConstants(finalTypes);
			final var className = c.getCanonicalName();
			if (bestCtor.getParameterCount() == 0) return new ObjectMeta(className, Meta.asSupplier(c, LOOKUP.unreflectConstructor(bestCtor)), finalSetters, keys, finalTypes, finalComps, enumConstants, engine.failOnUnknownProperties());
			final var factory = ConstructorGenerator.generate(c, finalTypes);
			if (totalProps == params.length)  return new ObjectMeta(className, factory,                              keys, finalTypes, finalComps, enumConstants, engine.failOnUnknownProperties());
			return                                   new ObjectMeta(className, factory, finalSetters, params.length, keys, finalTypes, finalComps, enumConstants, engine.failOnUnknownProperties());

		} catch (final Throwable e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofRecord(final InternalEngine engine, final Class<? extends Record> c) {
		final var comps     = c.getRecordComponents();
		final var metaComps = new ComponentMeta[comps.length];
		final var types     = new Class<?>[comps.length];
		for (var i = 0; i < comps.length; i++) {
			metaComps[i] = new ComponentMeta(comps[i].getName(), comps[i].getType());
			types    [i] = comps[i].getType();
		}
		try {
			return new ObjectMeta(c.getCanonicalName(), ConstructorGenerator.generate(c, types), FastKeyTable.build(metaComps), types, metaComps, buildEnumConstants(types), engine.failOnUnknownProperties());
		} catch (final Throwable e) {
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofEnum(final InternalEngine engine, final Class<?> c) {
		final var values = c.getEnumConstants();
		final ConstructorGenerator.ObjectArrayFactory factory = (objects, _) -> Meta.resolveEnum(values, objects[1]);
		return new ObjectMeta(c.getCanonicalName(), factory, ENUM_KEYS, ENUM_TYPES, ENUM_COMPS, null, engine.failOnUnknownProperties());
	}

	ObjectMeta() {
		this.pojoStart      = null;
		this.failOnUnknown  = false;
		this.className      = null;
		this.metaType       = TYPE_DEFECT;
		this.pojoSetters    = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.keys           = null;
		this.types          = null;
		this.components     = null;
		this.baseType       = null;
		this.enumConstants  = null;
	}

	private ObjectMeta(final String className, final Supplier<Object> pojoStart, final PropSetter[] pojoSetters, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enumConstants, final boolean failOnUnknown) {
		this.className      = className;
		this.pojoStart      = pojoStart;
		this.failOnUnknown  = failOnUnknown;
		this.metaType       = null == pojoStart ? TYPE_DEFECT : TYPE_POJO;
		this.pojoSetters    = pojoSetters;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.baseType       = null;
		this.enumConstants  = enumConstants;
	}

	private ObjectMeta(final String className, final ConstructorGenerator.ObjectArrayFactory factory, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enumConstants, final boolean failOnUnknown) {
		this.className      = className;
		this.metaType       = TYPE_RECORD;
		this.failOnUnknown  = failOnUnknown;
		this.pojoStart      = null;
		this.pojoSetters    = null;
		this.factory        = factory;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.enumConstants  = enumConstants;
	}

	private ObjectMeta(final String className, final ConstructorGenerator.ObjectArrayFactory factory, final PropSetter[] pojoSetters, final int ctorParamCount, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enumConstants, final boolean failOnUnknown) {
		this.className      = className;
		this.metaType       = TYPE_MIXED;
		this.failOnUnknown  = failOnUnknown;
		this.pojoStart      = null;
		this.pojoSetters    = pojoSetters;
		this.factory        = factory;
		this.ctorParamCount = ctorParamCount;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.enumConstants  = enumConstants;
	}

	ObjectMeta(final Class<?> mapValueType) {
		this.className      = "<MAP>";
		this.metaType       = TYPE_MAP;
		this.failOnUnknown  = false;
		this.pojoStart      = null;
		this.pojoSetters    = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = new FastKeyTable(new String[0], new int[0], 0);
		this.types          = new Class<?>[] {mapValueType};
		this.components     = new ComponentMeta[0];
		this.enumConstants  = null;
	}

	ObjectMeta(final String className, final Class<?>[] subclasses, final String[] keys, final Class<?>[] types, final boolean failOnUnknown) {
		this.className      = className;
		final var possibleComponents = new ComponentMeta[keys.length];
		this.failOnUnknown  = failOnUnknown;
		for (var i = 0; i < keys.length; i++) possibleComponents[i] = new ComponentMeta(keys[i], types[i]);
		this.metaType       = TYPE_SEALED;
		this.pojoStart      = null;
		this.pojoSetters    = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = subclasses;
		this.baseType       = null;
		this.keys           = FastKeyTable.build(possibleComponents);
		this.types          = types;
		this.components     = possibleComponents;
		this.enumConstants  = buildEnumConstants(types);
	}

	ComponentMeta[] components() { return components; }

	int prepareKey(final Object context, final String key) {
		if (metaType == TYPE_MAP) { ((ArrayBuilder) context).currentKey = key; return 0; }
		final var idx = keys.get(key);
		if (idx == -1 && failOnUnknown) throw new IllegalArgumentException("Unknown property " + key+ " in class "+className);
		return idx;
	}

	private static Object typeDefault(final Class<?> t) { return (t == boolean.class ? false : (t == char.class ? '\0' : 0)); }

	Class<?> type (final int index) { return types[metaType == TYPE_MAP ? 0 : index]; }

	Object start(final MetaPool s) {
		return switch (metaType) {
		case TYPE_MAP                -> s.takeBuilder();
		case TYPE_RECORD, TYPE_MIXED, TYPE_SEALED -> s.takeCtxArrays(types.length);
		case TYPE_POJO               -> pojoStart.get();
		default                      -> throw new IllegalArgumentException("T: "+metaType);
		};
	}

	void setLong(final MetaPool s, final Object context, final int index, final long v) {
		if (metaType == TYPE_RECORD || metaType == TYPE_MIXED || metaType == TYPE_SEALED) {
			((CtxArrays) context).primitives()[index] = v;
		} else if (metaType == TYPE_POJO) {
			final var setter = pojoSetters[index];
			if (setter != null) {
				if      (setter.longSetter   != null) setter.longSetter  .accept(context, v);
				else if (setter.intSetter    != null) setter.intSetter   .accept(context, (int) v);
				else if (setter.doubleSetter != null) setter.doubleSetter.accept(context, v);
				else if (setter.objSetter    != null) {
					final var t = types[index];
					if      (t == Integer.class) setter.objSetter.accept(context, Integer.valueOf((int)v));
					else if (t == Short  .class) setter.objSetter.accept(context, Short  .valueOf((short)v));
					else if (t == Byte   .class) setter.objSetter.accept(context, Byte   .valueOf((byte)v));
					else if (t == Float  .class) setter.objSetter.accept(context, Float  .valueOf(v));
					else if (t == Double .class) setter.objSetter.accept(context, Double .valueOf(v));
					else                         setter.objSetter.accept(context, Long   .valueOf(v));
				}
			}
		} else {
			set(s, context, index, Long.valueOf(v));
		}
	}

	void setDouble(final MetaPool s, final Object context, final int index, final double v)  {
		if (metaType == TYPE_RECORD || metaType == TYPE_MIXED || metaType == TYPE_SEALED) {
			((CtxArrays) context).primitives()[index] = Double.doubleToRawLongBits(v);
		} else if (metaType == TYPE_POJO) {
			final var setter = pojoSetters[index];
			if (setter != null) {
				if      (setter.doubleSetter != null) setter.doubleSetter.accept(context, v);
				else if (setter.longSetter   != null) setter.longSetter  .accept(context, (long) v);
				else if (setter.intSetter    != null) setter.intSetter   .accept(context, (int) v);
				else if (setter.objSetter    != null) {
					final var t = types[index];
					if      (t == Float  .class) setter.objSetter.accept(context, Float  .valueOf((float)v));
					else if (t == Integer.class) setter.objSetter.accept(context, Integer.valueOf((int)v));
					else if (t == Long   .class) setter.objSetter.accept(context, Long   .valueOf((long)v));
					else                         setter.objSetter.accept(context, Double .valueOf(v));
				}
			}
		} else {
			set(s, context, index, Double.valueOf(v));
		}
	}

	void setBoolean(final MetaPool s, final Object context, final int index, final boolean v) {
		if (metaType == TYPE_RECORD || metaType == TYPE_MIXED) {
			((CtxArrays) context).primitives()[index] = v ? 1L : 0L;
		} else if (metaType == TYPE_POJO) {
			final var setter = pojoSetters[index];
			if (setter != null) {
				if      (setter.boolSetter != null) setter.boolSetter.accept(context, v);
				else if (setter.objSetter  != null) setter.objSetter .accept(context, Boolean.valueOf(v));
			}
		} else {
			set(s, context, index, Boolean.valueOf(v));
		}
	}

	void set(final MetaPool s, final Object context, final int index, final Object value) {
		final var type = types[metaType == TYPE_MAP ? 0 : index];
		final var skip = s.engine().config().skipDefaultValues();

		final var v = switch(value) {
		case final CompactMap<?,?> t -> (t.isEmpty() ? null : t);
		case final CompactList<?>  t -> (t.isEmpty() ? null : t);
		case final Map<?,?>        t -> (t.isEmpty() ? null : t);
		case final Collection<?>   t -> (t.isEmpty() ? null : t);
		case final Integer         t -> (!skip || t.intValue   () != 0   || type.isPrimitive() ? t : null);
		case final Long            t -> (!skip || t.longValue  () != 0   || type.isPrimitive() ? t : null);
		case final Double          t -> (!skip || t.doubleValue() != 0.0 || type.isPrimitive() ? t : null);
		case final Number          t -> (!skip || t.doubleValue() != 0.0 || type.isPrimitive() ? t : null);
		case final Boolean         t -> (!skip || t                      || type.isPrimitive() ? t : null);
		case null                    -> type.isPrimitive() ? typeDefault(type): null;
		default                      -> value;
		};

		if(skip && null == v) return;
		switch (metaType) {
		case TYPE_MAP -> {
			final var b = (ArrayBuilder) context;
			if (b.cnt + 1 >= b.buf.length) {
				final var newBuf = s.takeArray(b.buf.length * 2);
				System.arraycopy(b.buf, 0, newBuf, 0, b.cnt);
				s.returnArray(b.buf);
				b.buf = newBuf;
			}
			b.buf[b.cnt++] = b.currentKey;
			b.buf[b.cnt++] = v;
			b.currentKey = null;
		}
		case TYPE_RECORD, TYPE_MIXED, TYPE_SEALED -> {
			final var arrays = (CtxArrays) context;
			if (type.isPrimitive()) {
				var primVal = 0L;
				if (v instanceof final Boolean b) primVal = b ? 1L : 0L;
				else if (v instanceof final Number n) {
					if      (type == double.class) primVal = Double.doubleToRawLongBits(n.doubleValue());
					else if (type == float .class) primVal = Float.floatToRawIntBits(n.floatValue());
					else                           primVal = n.longValue();
				}
				arrays.primitives()[index] = primVal;
			} else {
				var objVal = v;
				if (this.enumConstants != null && this.enumConstants[index] != null && objVal != null) {
					objVal = Meta.resolveEnum(this.enumConstants[index], objVal);
				}
				arrays.objects()[index] = objVal;
			}
		}
		case TYPE_POJO   -> {
			final var setter = pojoSetters[index];
			if (setter != null) {
				if (setter.objSetter != null) setter.objSetter.accept(context, v);
				else if (v instanceof final Number n) {
					if      (setter.intSetter    != null) setter.intSetter   .accept(context, n.intValue());
					else if (setter.longSetter   != null) setter.longSetter  .accept(context, n.longValue());
					else if (setter.doubleSetter != null) setter.doubleSetter.accept(context, n.doubleValue());
				} else if      ((v instanceof final Boolean b) && (setter.boolSetter   != null)) setter.boolSetter  .accept(context, b);
			}
		}
		default -> throw E_TYPE2;
		}
	}

	void endSet(final Object result, final int i, final CtxArrays arrays) {
		final var setter = pojoSetters[i];
		if (setter != null) {
			final var type = types[i];
			if (type.isPrimitive()) {
				final var primVal = arrays.primitives()[i];
				if      (setter.intSetter    != null) setter.intSetter   .accept(result, (int) primVal);
				else if (setter.longSetter   != null) setter.longSetter  .accept(result, primVal);
				else if (setter.doubleSetter != null) {
					if (type == double.class) setter.doubleSetter.accept(result, Double.longBitsToDouble(primVal));
					else                      setter.doubleSetter.accept(result, Float.intBitsToFloat((int)primVal));
				}
				else if (setter.boolSetter   != null) setter.boolSetter  .accept(result, primVal != 0L);
			} else if (arrays.objects()[i] != null && setter.objSetter != null) setter.objSetter.accept(result, arrays.objects()[i]);
		}
	}

	Object   end  (final MetaPool s, final Object context) throws Throwable {
		return switch (metaType) {
		case TYPE_MAP    -> { final var b = (ArrayBuilder) context; final var data = Arrays.copyOf(b.buf, b.cnt); s.returnBuilder(b); yield new CompactMap<>(data); }
		case TYPE_RECORD, TYPE_MIXED  -> {
			final var arrays = (CtxArrays) context;
			final var result = factory.create(arrays.objects(), arrays.primitives());
			if(pojoSetters != null) for (var i = ctorParamCount; i < pojoSetters.length; i++) endSet(result, i, arrays);
			s.returnCtxArrays(arrays);
			yield result;
		}
		case TYPE_POJO   -> context;
		case TYPE_SEALED -> SealedUnionMapper.end(s, context, baseType, permitted, keys, types);
		default          -> throw E_TYPE3;
		};
	}
}