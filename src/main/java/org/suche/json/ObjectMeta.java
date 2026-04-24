package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.suche.json.CompactMap.PRIMITIVE;
import org.suche.json.ConstructorGenerator.ObjectArrayFactory;
import org.suche.json.MetaPool.ParseContext;

interface ObjBooleanConsumer<T> { void accept(T t, boolean value); }

final class ObjectMeta {
	static final int IDX_GENERIC = 0, IDX_MAP = 1, IDX_COLLECTION = 2, IDX_SET = 3, IDX_OBJ_ARRAY = 4;

	static final long DESC_COLLECTION = EngineImpl.createTypeDesc(true, false, IDX_COLLECTION);
	static final long DESC_OBJ_ARRAY  = EngineImpl.createTypeDesc(true, false, IDX_OBJ_ARRAY);

	static final int IDX_CUSTOM_START = 16;

	static final int TYPE_INSTANTIATOR = 1;
	static final int TYPE_MAP = 2;
	private static final int TYPE_DEFECT = 3;
	private static final int TYPE_SEALED = 4;
	static final int TYPE_OBJ_ARRAY = 6;
	static final int TYPE_COLLECTION = 7;
	static final int TYPE_SET = 8;
	static final int PRIM_INT = 1, PRIM_LONG = 2, PRIM_DOUBLE = 3, PRIM_FLOAT = 4, PRIM_BOOLEAN = 5, PRIM_OTHER = 6;

	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	static final ObjectMeta DEFECT_FIRST = new ObjectMeta(-1);
	static final ObjectMeta DEFECT = new ObjectMeta(-1);
	static final RuntimeException E_DEFEKT2 = new RuntimeException("DEFEKT.2", null, false, false) { };
	static final ObjectMeta NULL = new ObjectMeta(null, null, -1);
	static final ObjectMeta GENERIC_MAP = new ObjectMeta(null, Object.class, IDX_MAP);
	private static final ComponentMeta[] ENUM_COMPS = { new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class), new ComponentMeta("value", String.class) };
	private static final Class<?>[] ENUM_TYPES = { String.class, String.class };
	private static final FastKeyTable ENUM_KEYS = FastKeyTable.build(ENUM_COMPS);

	static int getPrimId(final Class<?> type) {
		if (type == int.class) return PRIM_INT;
		if (type == long.class) return PRIM_LONG;
		if (type == double.class) return PRIM_DOUBLE;
		if (type == float.class) return PRIM_FLOAT;
		if (type == boolean.class) return PRIM_BOOLEAN;
		return PRIM_OTHER;
	}

	final int metaType;
	private final String className;
	private final boolean failOnUnknown;
	final Supplier<Object> pojoStart;
	final ConstructorGenerator.ObjectArrayFactory factory;
	private final IntFunction<Object> arrayCreator;
	final int ctorParamCount;
	private final FastKeyTable keys;
	final Class<?>[] types;
	private final Class<?> baseType;
	final Class<?>[] permitted;
	final Object[][] enumConstants;
	final boolean skipDefaultValues;
	private final int[] lastSeenSizeByDepth = new int[64];
	final int cacheIndex;
	final boolean needsPrims;

	private final long[] fieldDescriptors;
	final long componentDescriptor;

	long fieldDescriptor(final int idx) {
		if(fieldDescriptors.length == 0) return componentDescriptor;
		return fieldDescriptors[idx];
	}

	private int startSize(final int depth) {
		final var size = depth < 64 && depth >= 0 ? lastSeenSizeByDepth[depth] : 16;
		if (size < 1) return 2;
		if (size > 512) return 512;
		return size;
	}

	private void lastSize(final int depth, final int size) { if (depth >= 0 && depth < 64) lastSeenSizeByDepth[depth] = size; }

	record ComponentMeta(String name, Class<?> type) {
		ComponentMeta {
			if (name == null) throw new IllegalStateException("Missing name");
			if (type == null) throw new IllegalStateException("Missing type");
		}
	}

	final ComponentMeta[] components;

	static final record Prop(String name, boolean isField, Class<?> type, int ctorIdx, MethodHandle setterHandle) { }

	private static final int MOD_FINAL_OR_STATIC = Modifier.STATIC | Modifier.FINAL;

	private static long resolveDescriptor(final InternalEngine e, Class<?> type) {
		if (type == null) type = Object.class;
		final var isArray = type.isArray() || Collection.class.isAssignableFrom(type);
		final var isPrimArray = isArray && type.isArray() && type.componentType().isPrimitive();
		final var isPrimValue = !isArray && type.isPrimitive();

		Class<?> primTarget = null;
		if (isPrimArray) primTarget = type.componentType();
		else if (isPrimValue) primTarget = type;

		final int subIdx;
		if (primTarget != null) subIdx = getPrimId(primTarget);
		else if (e != null) {
			if (type.isArray()) subIdx = ((EngineImpl) e).getDynamicMetaId(type.componentType(), TYPE_OBJ_ARRAY);
			else if (Set       .class.isAssignableFrom(type)) subIdx = ((EngineImpl) e).getDynamicMetaId(Object.class, TYPE_SET);
			else if (Collection.class.isAssignableFrom(type)) subIdx = ((EngineImpl) e).getDynamicMetaId(Object.class, TYPE_COLLECTION);
			else subIdx = ((EngineImpl) e).metaIdOf(type);
		} else subIdx = IDX_GENERIC;
		return EngineImpl.createTypeDesc(isArray, primTarget != null, subIdx);
	}

	ObjectMeta(final InternalEngine e, final Class<?> compType, final int targetMetaType, final int cacheIndex) {
		this.cacheIndex = cacheIndex;
		this.className = targetMetaType == TYPE_OBJ_ARRAY ? "<ARRAY>" : (targetMetaType == TYPE_SET ? "<SET>" : "<COLLECTION>");
		this.metaType = targetMetaType;
		this.failOnUnknown = false;
		this.pojoStart = null;
		this.factory = null;
		this.arrayCreator = (compType != null && targetMetaType == TYPE_OBJ_ARRAY) ? size -> Array.newInstance(compType, size) : null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.baseType = null;
		this.keys = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types = new Class<?>[] { compType != null ? compType : Object.class };
		this.components = new ComponentMeta[0];
		this.enumConstants = null;
		this.skipDefaultValues = e.config().skipDefaultValues();
		this.needsPrims = compType != null && compType.isPrimitive();
		this.componentDescriptor = resolveDescriptor(e, compType);
		this.fieldDescriptors = new long[0];
	}

	private ObjectMeta(final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.pojoStart = null;
		this.failOnUnknown = false;
		this.className = null;
		this.metaType = TYPE_DEFECT;
		this.factory = null;
		this.arrayCreator = null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.keys = null;
		this.types = null;
		this.components = null;
		this.baseType = null;
		this.enumConstants = null;
		this.skipDefaultValues = false;
		this.needsPrims = false;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = new long[0];

	}

	private static final ConstructorGenerator.ObjectArrayFactory NO_FACTORY       = null;
	private static final int                                     NO_FACTORY_PARAM = 0;
	private static final Supplier<Object>                        NO_POJO_START    = null;

	ObjectMeta(final InternalEngine e, final String pClassName, final Supplier<Object> pStart, final ObjectArrayFactory pFactory, final int pStartCount
			, final FastKeyTable pKeys, final Class<?>[] pTypes, final ComponentMeta[] pComponents, final Object[][] pEnums, final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.className = pClassName;
		this.pojoStart = pStart;
		this.metaType = (pStart == null && pFactory == null) ? TYPE_DEFECT : TYPE_INSTANTIATOR;
		this.factory = pFactory;
		this.arrayCreator = null;
		this.ctorParamCount = pStartCount;
		this.permitted = null;
		this.baseType = null;
		this.keys = pKeys;
		this.types = pTypes;
		this.components = pComponents;
		this.enumConstants = pEnums;
		this.failOnUnknown = e.failOnUnknownProperties();
		this.skipDefaultValues = e.config().skipDefaultValues();
		var primFound = false;
		for (final var t : pTypes) if (t != null && t.isPrimitive()) { primFound = true; break; }
		this.needsPrims = primFound;
		this.componentDescriptor = 0L;
		if (pComponents != null && pComponents.length > 0) {
			this.fieldDescriptors = new long[pComponents.length];
			for (var i = 0; i < pComponents.length; i++) this.fieldDescriptors[i] = resolveDescriptor(e, pComponents[i].type());
		} else {
			this.fieldDescriptors = new long[0];
		}
	}

	ObjectMeta(final InternalEngine e, final Class<?> mapValueType, final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.className = "<MAP>";
		this.metaType = TYPE_MAP;
		this.failOnUnknown = false;
		this.pojoStart = null;
		this.factory = null;
		this.arrayCreator = null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.baseType = null;
		this.keys = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types = new Class<?>[] { mapValueType };
		this.components = new ComponentMeta[0];
		this.enumConstants = null;
		this.skipDefaultValues = e == null ? true : e.config().skipDefaultValues();
		this.needsPrims = mapValueType != null && mapValueType.isPrimitive();
		this.componentDescriptor = resolveDescriptor(e, mapValueType);
		this.fieldDescriptors = new long[] { this.componentDescriptor };
	}

	ObjectMeta(final InternalEngine e,final String pClassName, final Class<?>[] pSubclasses, final String[] pKeys, final Class<?>[] pTypes, final int pCacheIndex) {
		this.cacheIndex     = pCacheIndex;
		this.className      = pClassName;
		final var possibleComponents = new ComponentMeta[pKeys.length];
		this.failOnUnknown  = e.failOnUnknownProperties();
		for (var i = 0; i < pKeys.length; i++) possibleComponents[i] = new ComponentMeta(pKeys[i], pTypes[i]);
		this.metaType       = TYPE_SEALED;
		this.pojoStart      = null;
		this.factory        = null;
		this.arrayCreator   = null;
		this.ctorParamCount = 0;
		this.permitted      = pSubclasses;
		this.baseType       = null;
		this.keys           = FastKeyTable.build(possibleComponents);
		this.types          = pTypes;
		this.components     = possibleComponents;
		this.enumConstants  = buildEnumConstants(pTypes);
		this.skipDefaultValues = e.config().skipDefaultValues();
		this.needsPrims = false;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = new long[0];
	}

	@Deprecated(forRemoval = true, since = "Switch to getChildDescriptor / getComponentDescriptor")
	Class<?> type(final int index) { return types[1==types.length?0:index]; }
	long getChildDescriptor(final int index) { return (this.metaType == TYPE_MAP ? this.componentDescriptor : (fieldDescriptors == null ? 0L : fieldDescriptors[index])); }
	long getComponentDescriptor() { return componentDescriptor; }

	static ObjectMeta ofPojo(final InternalEngine engine, final Class<?> c, final int cacheIndex) {
		try {
			final var ctors = c.getDeclaredConstructors();
			Constructor<?> bestCtor = null;
			for (final var ctor : ctors) {
				if (ctor.getParameterCount() == 0) { bestCtor = ctor; break; }
				if (bestCtor == null || ctor.getParameterCount() > bestCtor.getParameterCount()) bestCtor = ctor;
			}
			if (bestCtor == null) return DEFECT_FIRST;
			if (!bestCtor.canAccess(null)) bestCtor.setAccessible(true);
			final var params = bestCtor.getParameters();
			final var props = pojoProps(c, params);
			final var totalProps = props.size();
			final var finalComps = new ComponentMeta[totalProps];
			final var finalTypes = new Class<?>[totalProps];
			var setterCounter = params.length;
			for (final var entry : props.entrySet()) {
				final var targetIdx = entry.getValue().ctorIdx != -1 ? entry.getValue().ctorIdx : setterCounter++;
				finalComps[targetIdx] = new ComponentMeta(entry.getKey(), entry.getValue().type);
				finalTypes[targetIdx] = entry.getValue().type;
			}
			final var keys = FastKeyTable.build(finalComps);
			final var enumConstants = buildEnumConstants(finalTypes);
			if (bestCtor.getParameterCount() == 0) return new ObjectMeta(engine, c.getCanonicalName(), Meta.asSupplier(c, LOOKUP.unreflectConstructor(bestCtor)), NO_FACTORY, NO_FACTORY_PARAM, keys, finalTypes, finalComps, enumConstants, cacheIndex);
			return new ObjectMeta(engine, c.getCanonicalName(), NO_POJO_START, ConstructorGenerator.generate(c, finalTypes), params.length, keys, finalTypes, finalComps, enumConstants, cacheIndex);
		} catch (final Throwable e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofRecord(final InternalEngine engine, final Class<? extends Record> c, final int cacheIndex) {
		final var comps = c.getRecordComponents();
		final var metaComps = new ComponentMeta[comps.length];
		final var types = new Class<?>[comps.length];
		try {
			for (var i = 0; i < comps.length; i++) {
				types[i] = comps[i].getType();
				metaComps[i] = new ComponentMeta(comps[i].getName(), types[i]);
			}
			return new ObjectMeta(engine, c.getCanonicalName(), NO_POJO_START, ConstructorGenerator.generate(c, types), 0, FastKeyTable.build(metaComps), types, metaComps, buildEnumConstants(types), cacheIndex);
		} catch (final Throwable e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofEnum(final InternalEngine e, final Class<?> c, final int cacheIndex) {
		final var values = c.getEnumConstants();
		return new ObjectMeta(e, c.getCanonicalName(), NO_POJO_START, (objects, _) -> Meta.resolveEnum(values, objects[1]), 0, ENUM_KEYS, ENUM_TYPES, ENUM_COMPS, null, cacheIndex);
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
		final var props = new LinkedHashMap<String, Prop>();
		for (var i = 0; i < params.length; i++) props.put(params[i].getName(), new Prop(params[i].getName(), false, params[i].getType(), i, null));
		for (final var m : c.getMethods()) {
			if (m.getParameterCount() != 1 || m.getName().length() < 4 || !m.getName().startsWith("set")) continue;
			final var name = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
			if (!props.containsKey(name)) {
				m.setAccessible(true);
				props.put(name, new Prop(name, false, m.getParameterTypes()[0], -1, LOOKUP.unreflect(m)));
			}
		}
		for (final var f : c.getDeclaredFields()) {
			if (0 != (f.getModifiers() & MOD_FINAL_OR_STATIC)) continue;
			if (!props.containsKey(f.getName())) {
				f.setAccessible(true);
				props.put(f.getName(), new Prop(f.getName(), true, f.getType(), -1, LOOKUP.unreflectSetter(f)));
			}
		}
		return props;
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
		final var idx = keys.get(FastKeyTable.computeHash(b, 0, b.length), b, 0, b.length);
		if (idx == -1 && failOnUnknown) throw new IllegalArgumentException("Unknown property " + key + " in class " + className);
		return idx;
	}

	Object start(final MetaPool s) {
		return switch (metaType) {
		case TYPE_MAP, TYPE_OBJ_ARRAY, TYPE_COLLECTION -> s.takeContext(startSize(s.depth()), TYPE_MAP == metaType);
		case TYPE_INSTANTIATOR -> {
			final var len = fieldDescriptors != null ? fieldDescriptors.length : 0;
			final var ctx = s.takeContext(len, false);
			if (ctx.objs == null) ctx.objs = s.takeArray(len);
			if (needsPrims && ctx.prims == null) ctx.prims = s.takeLongArray(len);
			ctx.cnt = len;
			yield ctx;
		}
		case TYPE_SET -> new HashSet<>();
		default -> throw new IllegalArgumentException();
		};
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
			if (ctx.objs == null) yield null; // No data -> no key -> no prims => empty => null
			final var data = Arrays.copyOf(ctx.objs, ctx.cnt);
			final var prims = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, ctx.cnt >> 1);
			s.returnContext(ctx);
			yield new CompactMap<>(ctx.singleType, data, prims);
		}
		case TYPE_OBJ_ARRAY -> {
			final var ctx = (ParseContext) context;
			if (skipDefaultValues && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			lastSize(s.depth(), ctx.cnt);
			final var result = arrayCreator.apply(ctx.cnt);
			if (ctx.objs != null) System.arraycopy(ctx.objs, 0, result, 0, ctx.cnt);
			else if (ctx.prims != null) {
				// Primitive Werte in Ziel-Array boxen (z.B. für Double[] vs double[])
				if (ctx.singleType == MetaPool.T_DOUBLE) for (var i = 0; i < ctx.cnt; i++) Array.set(result, i, Double.longBitsToDouble(ctx.prims[i]));
				else if (ctx.singleType == MetaPool.T_LONG) for (var i = 0; i < ctx.cnt; i++) Array.set(result, i, ctx.prims[i]);
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
				Arrays.fill(data, ctx.singleType == MetaPool.T_LONG ? PRIMITIVE.LONG : PRIMITIVE.DOUBLE);
				primsData = Arrays.copyOf(ctx.prims, ctx.cnt);
			}
			s.returnContext(ctx);
			yield new CompactList<>(ctx.singleType, data, primsData);
		}
		case TYPE_SET -> skipDefaultValues && ((Collection<Object>) context).isEmpty() ? null : context;
		case TYPE_SEALED -> SealedUnionMapper.end(s, context, baseType, permitted, keys, types);
		default -> throw new IllegalArgumentException();
		};
	}

	@SuppressWarnings("unchecked")
	void setLong(final MetaPool s, final Object context, final int index, final long v) {
		if (index < 0) throw new IllegalStateException();
		if (skipDefaultValues && v == 0L) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR -> ((ParseContext) context).prims[index] = v;
		case TYPE_MAP -> primKeyValue(context, s, PRIMITIVE.LONG, v);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> primIdxValue(context, s, PRIMITIVE.LONG, MetaPool.T_LONG, v, index);
		case TYPE_SET -> ((Collection<Object>) context).add(v);
		default -> set(s, context, index, v);
		}
	}

	@SuppressWarnings("unchecked")
	void setDouble(final MetaPool s, final Object context, final int index, final double v) {
		if (index < 0) throw new IllegalStateException();
		if (skipDefaultValues && v == 0.0) return;
		final var bits = Double.doubleToRawLongBits(v);
		switch (metaType) {
		case TYPE_INSTANTIATOR -> ((ParseContext) context).prims[index] = bits;
		case TYPE_MAP -> primKeyValue(context, s, PRIMITIVE.DOUBLE, bits);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> primIdxValue(context, s, PRIMITIVE.DOUBLE, MetaPool.T_DOUBLE, bits, index);
		case TYPE_SET -> ((Collection<Object>) context).add(v);
		default -> set(s, context, index, v);
		}
	}

	@SuppressWarnings("unchecked")
	void set(final MetaPool s, final Object context, final int index, Object value) {
		if (index < 0) throw new IllegalStateException();
		final var targetType = (components != null && index < components.length) ? components[index].type() : null;
		if (targetType == boolean.class && value != null) { setLong(s, context, index, ((Boolean) value) ? 1 : 0); return; }
		if (value == null) {
			if (skipDefaultValues && metaType != TYPE_OBJ_ARRAY && metaType != TYPE_COLLECTION) return;
			if (targetType != null && targetType.isPrimitive()) value = (targetType == boolean.class ? Boolean.FALSE : 0);
		}
		if (this.enumConstants != null && index < enumConstants.length && this.enumConstants[index] != null) value = Meta.resolveEnum(this.enumConstants[index], value);
		switch (metaType) {
		case TYPE_INSTANTIATOR -> ((ParseContext) context).objs[index] = value;
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			if (ctx.objs == null || ctx.cnt + 2 > ctx.objs.length) ctx.ensureObjs(s, Math.max(ctx.cnt + 2, startSize(s.depth())));
			ctx.objs[ctx.cnt++] = ctx.currentKey;
			ctx.objs[ctx.cnt++] = value;
			ctx.currentKey = null;
		}
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> {
			final var ctx = (ParseContext) context;
			ctx.upgradeToMixed(s, index + 1);
			if (ctx.objs == null || index >= ctx.objs.length) ctx.ensureObjs(s, index + 1);
			ctx.objs[index] = value;
			if (index >= ctx.cnt) ctx.cnt = index + 1;
		}
		case TYPE_SET -> ((Collection<Object>) context).add(value);
		}
	}

	private void primKeyValue(final Object context, final MetaPool s, final PRIMITIVE type, final long value) {
		final var ctx = (ParseContext) context;
		final var valIdx = (ctx.cnt >> 1);
		if (ctx.objs == null || ctx.cnt + 1 >= ctx.objs.length) ctx.ensureObjs(s, ctx.cnt + 2);
		if (ctx.prims == null || valIdx >= ctx.prims.length) ctx.ensurePrims(s, valIdx + 1);
		ctx.objs[ctx.cnt++] = ctx.currentKey;
		ctx.objs[ctx.cnt++] = type;
		ctx.prims[valIdx] = value;
		ctx.currentKey = null;
	}

	private void primIdxValue(final Object context, final MetaPool s, final PRIMITIVE t, final byte type, final long value, final int index) {
		final var ctx = (ParseContext) context;
		if (ctx.singleType == MetaPool.T_EMPTY) ctx.singleType = type;
		else if (ctx.singleType == MetaPool.T_LONG && type == MetaPool.T_DOUBLE) ctx.upgradeToDouble();
		if (ctx.singleType == MetaPool.T_MIXED) {
			ctx.ensureObjs(s, index + 1);
			ctx.ensurePrims(s, index + 1);
			ctx.objs[index] = t;
			ctx.prims[index] = value;
		} else {
			ctx.ensurePrims(s, index + 1);
			ctx.prims[index] = (ctx.singleType == MetaPool.T_DOUBLE && type == MetaPool.T_LONG) ? Double.doubleToRawLongBits(value) : value;
		}
		if (index >= ctx.cnt) ctx.cnt = index + 1;
	}
}