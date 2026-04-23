package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.function.Supplier;

import org.suche.annotation.NonNull;
import org.suche.json.CompactMap.PRIMITIVE;
import org.suche.json.ConstructorGenerator.ObjectArrayFactory;
import org.suche.json.MetaPool.ParseContext;

interface ObjBooleanConsumer<T> { void accept(T t, boolean value); }

public final class ObjectMeta {
	static final int TYPE_INSTANTIATOR = 1;
	private static final int                  TYPE_MAP        = 2;
	private static final int                  TYPE_DEFECT     = 3;
	private static final int                  TYPE_SEALED     = 4;
	static         final int                  TYPE_OBJ_ARRAY  = 6;
	static         final int                  TYPE_COLLECTION = 7;
	static         final int                  TYPE_SET        = 8;

	private static final MethodHandles.Lookup LOOKUP        = MethodHandles.lookup();
	static         final ObjectMeta           DEFECT_FIRST  = new ObjectMeta();
	static         final ObjectMeta           DEFECT        = new ObjectMeta();
	static         final RuntimeException     E_DEFEKT2     = new RuntimeException("DEFEKT.2", null, false, false) { };
	static         final ObjectMeta           NULL          = new ObjectMeta(null, null);
	static         final ObjectMeta           GENERIC_MAP   = new ObjectMeta(null, Object.class);
	private static final ComponentMeta[]      ENUM_COMPS    =  { new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class), new ComponentMeta("value", String.class) };
	private static final Class<?>     []      ENUM_TYPES    =  { String.class, String.class };
	private static final FastKeyTable         ENUM_KEYS     = FastKeyTable.build(ENUM_COMPS);

	final int metaType;
	private final String className;
	private final boolean failOnUnknown;
	final Supplier<Object> pojoStart;
	final ConstructorGenerator.ObjectArrayFactory factory;
	final int          ctorParamCount;
	private final FastKeyTable keys          ;
	private final Class<?>[]   types         ;
	private final Class<?>     baseType      ;
	final Class<?>[]   permitted     ;
	final Object[][]   enumConstants ;
	final boolean skipDefaultValues;
	private final int[] lastSeenSizeByDepth = new int[64];

	private int startSize(final int depth) {
		final var size = depth < 64 && depth >= 0 ? lastSeenSizeByDepth[depth] : 16;
		if(size <   1) return 2;
		if(size > 512) return 512;
		return size;
	}

	private void lastSize(final int depth, final int size) { if(depth >= 0 && depth < 64) lastSeenSizeByDepth[depth] = size; }

	record ComponentMeta(String name, Class<?> type) {
		public ComponentMeta {
			if(name == null) throw new IllegalStateException("Missing name");
			if(type == null) throw new IllegalStateException("Missing type");
		}
	}

	final ComponentMeta[] components;

	static final record Prop(String name, boolean isField, Class<?> type, int ctorIdx, MethodHandle setterHandle) { }

	private static final int MOD_FINAL_OR_STATIC = Modifier.STATIC | Modifier.FINAL;

	ObjectMeta(final InternalEngine e, final Class<?> compType, final int targetMetaType) {
		this.className      = targetMetaType == TYPE_OBJ_ARRAY ? "<ARRAY>" : (targetMetaType == TYPE_SET ? "<SET>" : "<COLLECTION>");
		this.metaType       = targetMetaType;
		this.failOnUnknown  = false;
		this.pojoStart      = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types          = new Class<?>[] { compType != null ? compType : Object.class };
		this.components     = new ComponentMeta[0];
		this.enumConstants  = null;
		this.skipDefaultValues = e.config().skipDefaultValues();
	}

	ObjectMeta() {
		this.pojoStart         = null;
		this.failOnUnknown     = false;
		this.className         = null;
		this.metaType          = TYPE_DEFECT;
		this.factory           = null;
		this.ctorParamCount    = 0;
		this.permitted         = null;
		this.keys              = null;
		this.types             = null;
		this.components        = null;
		this.baseType          = null;
		this.enumConstants     = null;
		this.skipDefaultValues = false;
	}

	private ObjectMeta(final InternalEngine e,final String className, final Supplier<Object> pojoStart, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enumConstants) {
		this.className      = className;
		this.pojoStart      = pojoStart;
		this.metaType       = null == pojoStart ? TYPE_DEFECT : TYPE_INSTANTIATOR;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.baseType       = null;
		this.enumConstants  = enumConstants;
		this.failOnUnknown  = e.failOnUnknownProperties();
		this.skipDefaultValues = e.config().skipDefaultValues();
	}

	private ObjectMeta(final InternalEngine e,final String className, final ConstructorGenerator.ObjectArrayFactory factory, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enumConstants) {
		this.className      = className;
		this.metaType       = TYPE_INSTANTIATOR;
		this.pojoStart      = null;
		this.factory        = factory;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.enumConstants  = enumConstants;
		this.failOnUnknown  = e.failOnUnknownProperties();
		this.skipDefaultValues = e.config().skipDefaultValues();
	}

	private ObjectMeta(final InternalEngine e,final String className, final ObjectArrayFactory factory, final int ctorParamCount, final FastKeyTable keys, final Class<?>[] types, final ComponentMeta[] components, final Object[][] enums) {
		this.className      = className;
		this.metaType       = TYPE_INSTANTIATOR;
		this.pojoStart      = null;
		this.factory        = factory;
		this.ctorParamCount = ctorParamCount;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = keys;
		this.types          = types;
		this.components     = components;
		this.enumConstants  = enums;
		this.failOnUnknown  = e.failOnUnknownProperties();
		this.skipDefaultValues = e.config().skipDefaultValues();
	}

	ObjectMeta(final InternalEngine e,final Class<?> mapValueType) {
		this.className      = "<MAP>";
		this.metaType       = TYPE_MAP;
		this.failOnUnknown  = false;
		this.pojoStart      = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = null;
		this.keys           = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types          = new Class<?>[] {mapValueType};
		this.components     = new ComponentMeta[0];
		this.enumConstants  = null;
		this.skipDefaultValues = e == null ? true : e.config().skipDefaultValues();
	}

	ObjectMeta(final InternalEngine e,final String className, final Class<?>[] subclasses, final String[] keys, final Class<?>[] types, final boolean failOnUnknown) {
		this.className      = className;
		final var possibleComponents = new ComponentMeta[keys.length];
		this.failOnUnknown  = failOnUnknown;
		for (var i = 0; i < keys.length; i++) possibleComponents[i] = new ComponentMeta(keys[i], types[i]);
		this.metaType       = TYPE_SEALED;
		this.pojoStart      = null;
		this.factory        = null;
		this.ctorParamCount = 0;
		this.permitted      = subclasses;
		this.baseType       = null;
		this.keys           = FastKeyTable.build(possibleComponents);
		this.types          = types;
		this.components     = possibleComponents;
		this.enumConstants  = buildEnumConstants(types);
		this.skipDefaultValues = e.config().skipDefaultValues();
	}

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

	private static LinkedHashMap<String, Prop> pojoProps(final Class<?> c, final Parameter[] params) throws IllegalAccessException {
		final var props  = new LinkedHashMap<String, Prop>();

		for (var i = 0; i < params.length; i++) {
			final var p = params[i];
			props.put(p.getName(), new Prop(p.getName(), false, p.getType(), i, null));
		}

		for (var i = 0; i < params.length; i++) {
			final var p = params[i];
			props.put(p.getName(), new Prop(p.getName(), false, p.getType(), i, null));
		}

		for (final var m : c.getMethods()) {
			if(m.getParameterCount() != 1 || m.getName().length() < 4 || !m.getName().startsWith("set")) continue;
			final var name = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
			if(!props.containsKey(name)) {
				m.setAccessible(true);
				props.put(name, new Prop(name, false, m.getParameterTypes()[0], -1, LOOKUP.unreflect(m)));
			}
		}

		for (final var f : c.getDeclaredFields()) {
			if (0 != (f.getModifiers() & MOD_FINAL_OR_STATIC)) continue;
			final var name = f.getName();
			if(!props.containsKey(name)) {
				f.setAccessible(true);
				props.put(name, new Prop(name, true, f.getType(), -1, LOOKUP.unreflectSetter(f)));
			}
		}
		return props;
	}

	static ObjectMeta ofPojo(final InternalEngine engine, final Class<?> c) {
		try {
			final var ctors = c.getDeclaredConstructors();
			Constructor<?> bestCtor = null;
			for (final var ctor : ctors) {
				if (ctor.getParameterCount() == 0) { bestCtor = ctor; break; }
				if (bestCtor == null || ctor.getParameterCount() > bestCtor.getParameterCount()) bestCtor = ctor;
			}
			if (bestCtor == null) return DEFECT_FIRST;
			if (!bestCtor.canAccess(null)) bestCtor.setAccessible(true);

			final var params        = bestCtor.getParameters();
			final var props         = pojoProps(c, params);
			final var totalProps    = props.size();
			final var finalComps    = new ComponentMeta[totalProps];
			final var finalTypes    = new Class<?>     [totalProps];
			var setterCounter = params.length;

			for (final var entry : props.entrySet()) {
				final var name = entry.getKey();
				final var prop = entry.getValue();
				final var targetIdx = prop.ctorIdx != -1 ? prop.ctorIdx : setterCounter++;
				finalComps[targetIdx] = new ComponentMeta(name, prop.type);
				finalTypes[targetIdx] = prop.type;
			}

			final var keys = FastKeyTable.build(finalComps);
			final var enumConstants = buildEnumConstants(finalTypes);
			final var className = c.getCanonicalName();

			if (bestCtor.getParameterCount() == 0)
				return new ObjectMeta(engine, className, Meta.asSupplier(c, LOOKUP.unreflectConstructor(bestCtor)), keys, finalTypes, finalComps, enumConstants);

			final var factory = ConstructorGenerator.generate(c, finalTypes);
			return new ObjectMeta(engine, className, factory, params.length, keys, finalTypes, finalComps, enumConstants);

		} catch (final Throwable e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofRecord(final InternalEngine engine, final Class<? extends Record> c) {
		final var comps     = c.getRecordComponents();
		final var metaComps = new ComponentMeta[comps.length];
		final var types     = new Class<?>[comps.length];

		try {
			for (var i = 0; i < comps.length; i++) {
				final Class<?> type = comps[i].getType();
				metaComps[i] = new ComponentMeta(comps[i].getName(), type);
				types[i] = type;
			}
			return new ObjectMeta(engine, c.getCanonicalName(), ConstructorGenerator.generate(c, types),  0, FastKeyTable.build(metaComps), types, metaComps, buildEnumConstants(types));
		} catch (final Throwable e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofEnum(final InternalEngine e, final Class<?> c) {
		final var values = c.getEnumConstants();
		final ConstructorGenerator.ObjectArrayFactory factory = (objects, _) -> Meta.resolveEnum(values, objects[1]);
		return new ObjectMeta(e, c.getCanonicalName(), factory, ENUM_KEYS, ENUM_TYPES, ENUM_COMPS, null);
	}

	ComponentMeta[] components() { return components; }

	int prepareKey(final int hash, final byte[] buffer, final int off, final int len) {
		if (metaType == TYPE_MAP) return -1;
		final var idx = keys.get(hash, buffer, off, len);
		if (idx == -1 && failOnUnknown) throw new IllegalArgumentException("Unknown property in class " + className);
		return idx;
	}

	int prepareKey(final Object context, final String key) {
		if (metaType == TYPE_MAP) { ((ParseContext) context).currentKey = key; return 0; }
		final var b = key.getBytes(StandardCharsets.UTF_8);
		final var l = b.length;
		final var h = FastKeyTable.computeHash(b, 0, l);
		final var idx = keys.get(h,b,0,l);
		if (idx == -1 && failOnUnknown) throw new IllegalArgumentException("Unknown property " + key+ " in class "+className);
		return idx;
	}

	Class<?> type(final int index) { return  types[1==types.length?0:index]; }

	private static Object typeDefault(final Class<?> t) { return (t == boolean.class ? Boolean.FALSE : (t == char.class ? '\0' : 0)); }

	Object start(@NonNull final MetaPool s) {
		return switch (metaType) {
		case TYPE_MAP, TYPE_OBJ_ARRAY, TYPE_COLLECTION -> s.takeContext(startSize(s.depth()), TYPE_MAP==metaType);
		case TYPE_INSTANTIATOR -> {
			final var ctx = s.takeContext(types.length, false);
			// objs MUSS geholt werden, da die Factory es in der Signatur erwartet
			if (ctx.objs == null) ctx.objs = s.takeArray(types.length);
			// prims NUR holen, wenn das POJO wirklich primitive Felder hat!
			var needsPrims = false;
			for (final var t : types) if (t.isPrimitive()) { needsPrims = true; break; }
			if (needsPrims && ctx.prims == null) ctx.prims = s.takeLongArray(types.length);

			ctx.cnt = types.length;
			yield ctx;
		}
		case TYPE_SET -> new HashSet<>();
		default -> throw new IllegalArgumentException("T: "+metaType);
		};
	}

	void primKeyValue(final Object context, final MetaPool s, final PRIMITIVE type, final long value) {
		final var ctx = (ParseContext) context;
		final var valIdx = (ctx.cnt >> 1);
		if (ctx.objs == null || ctx.cnt + 1 >= ctx.objs.length)
			ctx.ensureObjs(s, ctx.objs == null ? Math.max(16, startSize(s.depth()) << 1) : ctx.cnt + 2);
		if (ctx.prims == null || valIdx >= ctx.prims.length) ctx.ensurePrims(s, Math.max(valIdx + 1, ctx.objs.length >> 1));
		ctx.objs[ctx.cnt++] = ctx.currentKey;
		ctx.objs[ctx.cnt++] = type;
		ctx.prims[valIdx] = value;
		ctx.currentKey = null;
	}

	void primIdxValue(final Object context, final MetaPool s, PRIMITIVE t, byte type, long value, final int index) {
		final var ctx = (ParseContext) context;
		if (ctx.singleType == MetaPool.T_EMPTY) ctx.singleType = type;
		else if (ctx.singleType == MetaPool.T_LONG && type == MetaPool.T_DOUBLE) {
			final var d = Double.longBitsToDouble(value);
			if(d == ((long)d)) {
				t     = PRIMITIVE.LONG;
				type  = MetaPool.T_LONG;
				value = (long) d;
			} else  ctx.upgradeToDouble();
		}
		final var reqCap = Math.max(index + 1, startSize(s.depth()));
		if (ctx.singleType == MetaPool.T_MIXED) {
			ctx.ensureObjs(s, reqCap);
			ctx.ensurePrims(s, reqCap);
			ctx.objs [index] = t;
			ctx.prims[index] = value;
		} else {
			ctx.ensurePrims(s, reqCap);
			final var doubleToLong = (ctx.singleType == MetaPool.T_DOUBLE && type == MetaPool.T_LONG);
			ctx.prims[index] = doubleToLong ? Double.doubleToRawLongBits(value) : value;
		}
		if (index >= ctx.cnt) ctx.cnt = index + 1;
	}

	@SuppressWarnings("unchecked")
	void setLong(final MetaPool s, final Object context, final int index, final long v) {
		if (skipDefaultValues && v == 0L) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR               -> ((ParseContext) context).prims[index] = v;
		case TYPE_MAP                        -> primKeyValue(context, s, PRIMITIVE.LONG, v);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> primIdxValue(context, s, PRIMITIVE.LONG, MetaPool.T_LONG, v, index);
		case TYPE_SET                        -> ((Collection<Object>) context).add(v);
		default                              -> set(s, context, index, Long.valueOf(v));
		}
	}

	@SuppressWarnings("unchecked")
	void setDouble(final MetaPool s, final Object context, final int index, final double v)  {
		if (skipDefaultValues && v == 0.0) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR               -> ((ParseContext) context).prims[index] = Double.doubleToRawLongBits(v);
		case TYPE_MAP                        -> primKeyValue(context, s, PRIMITIVE.DOUBLE, Double.doubleToRawLongBits(v));
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> primIdxValue(context, s, PRIMITIVE.DOUBLE, MetaPool.T_DOUBLE,  Double.doubleToRawLongBits(v), index);
		case TYPE_SET                        -> ((Collection<Object>) context).add(v);
		default                              -> set(s,                context, index, Double.valueOf(v));
		}
	}

	@SuppressWarnings("unchecked")
	void set(final MetaPool s, final Object context, final int index, Object value) {
		final var type = type(index);
		if(type == boolean.class && value != null) {
			setLong(s, context, index, ((Boolean)value) ? 1 : 0);
			return;
		}
		if (value == null) {
			if (skipDefaultValues || !type.isPrimitive()) return;
			value = typeDefault(type);
		} else if (skipDefaultValues && ((value instanceof final Number n  && n.doubleValue() == 0.0) || (value instanceof final Boolean b && !b.booleanValue()))) return;

		if (this.enumConstants != null && this.enumConstants[index] != null) value = Meta.resolveEnum(this.enumConstants[index], value);

		switch (metaType) {
		case TYPE_INSTANTIATOR -> ((ParseContext) context).objs[index] = value;
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			final var objSize = ctx.cnt + 2;
			if(ctx.objs == null || objSize > ctx.objs.length) ctx.ensureObjs (s, Math.max(objSize, startSize(s.depth())));
			ctx.objs[ctx.cnt++] = ctx.currentKey;
			ctx.objs[ctx.cnt++] = value;
			ctx.currentKey = null;
		}
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> {
			final var ctx = (ParseContext) context;
			final var reqCap = Math.max(index + 1, startSize(s.depth()));
			ctx.upgradeToMixed(s, reqCap);
			if (ctx.objs == null || index >= ctx.objs.length) ctx.ensureObjs(s, reqCap);
			ctx.objs[index] = value;
			if (index >= ctx.cnt) ctx.cnt = index + 1;
		}
		case TYPE_SET -> ((Collection<Object>) context).add(value);
		default -> throw new IllegalArgumentException();
		}
	}

	@SuppressWarnings("unchecked")
	Object end(final MetaPool s, final Object context) throws Throwable {
		return switch (metaType) {
		case TYPE_INSTANTIATOR -> {
			final var ctx = (ParseContext) context;
			final var result = factory.create(ctx.objs, ctx.prims);
			s.returnContext(ctx);
			yield result;
		}
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			if (skipDefaultValues && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			if(ctx.objs  == null) yield null; // No data -> no key -> no prims => empty => null
			final var data  = Arrays.copyOf(ctx.objs, ctx.cnt);
			final var prims = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, ctx.cnt >> 1);
			s.returnContext(ctx);
			yield new CompactMap<>(ctx.singleType, data, prims);
		}
		case TYPE_OBJ_ARRAY -> {
			final var ctx = (ParseContext) context;
			if (skipDefaultValues && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			lastSize(s.depth(), ctx.cnt);
			final var result = java.lang.reflect.Array.newInstance(types[0], ctx.cnt);
			if (ctx.objs != null) {
				System.arraycopy(ctx.objs, 0, result, 0, ctx.cnt);
			} else if (ctx.prims != null) {
				// Primitive Werte in Ziel-Array boxen (z.B. für Double[] vs double[])
				if (ctx.singleType == MetaPool.T_DOUBLE) {
					for (var i = 0; i < ctx.cnt; i++) java.lang.reflect.Array.set(result, i, Double.longBitsToDouble(ctx.prims[i]));
				} else if (ctx.singleType == MetaPool.T_LONG) {
					for (var i = 0; i < ctx.cnt; i++) java.lang.reflect.Array.set(result, i, ctx.prims[i]);
				}
			}
			s.returnContext(ctx);
			yield result;
		}
		case TYPE_COLLECTION -> {
			final var ctx = (ParseContext) context;
			if (skipDefaultValues && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			lastSize(s.depth(), ctx.cnt);
			final Object[] data;
			final long[] primsData;
			if (ctx.singleType == MetaPool.T_MIXED || ctx.singleType == MetaPool.T_EMPTY) {
				data = ctx.objs == null ? new Object[0] : Arrays.copyOf(ctx.objs, ctx.cnt);
				primsData = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, ctx.cnt);
			} else {
				data = new Object[ctx.cnt];
				if (ctx.singleType == MetaPool.T_LONG) Arrays.fill(data, PRIMITIVE.LONG);
				else Arrays.fill(data, PRIMITIVE.DOUBLE);
				primsData = Arrays.copyOf(ctx.prims, ctx.cnt);
			}
			s.returnContext(ctx);
			yield new CompactList<>(ctx.singleType,data, primsData);
		}
		case TYPE_SET -> skipDefaultValues && ((Collection<Object>) context).isEmpty() ? null : context;
		case TYPE_SEALED -> SealedUnionMapper.end(s, context, baseType, permitted, keys, types);
		default -> throw new IllegalArgumentException();
		};
	}
}