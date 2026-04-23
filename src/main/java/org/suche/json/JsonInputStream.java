package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.suche.json.ObjectMeta.ComponentMeta;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final byte TYPE_OBJECT = 1;
	private static final byte TYPE_ARRAY  = 2;
	private static final byte TYPE_COL    = 3;
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);

	static final class CTX  {
		byte     type;
		Object   obj;
		Object   meta;
		int      targetIdx;
		Class<?> targetType;

		void set(final byte pType, final Object pObj, final Object pMeta, final int pTargetIdx, final Class<?> pTargetType) {
			this.type       = pType;
			this.obj        = pObj;
			this.meta       = pMeta;
			this.targetIdx  = pTargetIdx;
			this.targetType = pTargetType;
		}

		public void clean() {
			this.obj        = null;
			this.meta       = null;
			this.targetType = null;
		}
	}

	static final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack = stack64;
		int   depth = -1;

		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }

		void push(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(type, obj, meta, targetIdx, targetType);
		}

		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clean();
			if (stack.length > 64) stack = stack64;
			depth = -1;
		}
	}

	private ObjectMeta defaultMapMeta;
	private final CtxStack engineStack = new CtxStack();

	private Class<?>   lastResolvedClass;
	private ObjectMeta lastResolvedMeta;
	private int        lastArraySize = 16;

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.engineStack.depth = -1;
		s.lastArraySize = 16;
		s.defaultMapMeta = engine.metaOf(Map.class);
		return s;
	}

	@Override public void close() throws IOException {
		engineStack.clear();
		depth             = -1;
		lastResolvedClass = null;
		lastResolvedMeta  = null;
		if (internPool != null) internPool.clear();
		Arrays.fill(onceInternal, 0, onceInternalSize, null);
		onceInternalSize = 0;
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	private void internalComponentKeys(final ComponentMeta[] components) {
		for(final var e : components) {
			final var src = e.name().getBytes(StandardCharsets.UTF_8);
			var hash = 0;
			var isAscii = true;
			for (final byte b : src) {
				hash = 31 * hash + b;
				if(b < 0) isAscii = false;
			}
			internBytes(src, 0, src.length, hash, isAscii);
		}
	}

	ObjectMeta metaOf(final Class<?> clazz) {
		if(clazz == lastResolvedClass) return lastResolvedMeta;

		if(engine.metaOf(clazz) instanceof final ObjectMeta r) {
			this.lastResolvedClass = clazz;
			this.lastResolvedMeta  = r;

			if(r .components() == null || r.components().length == 0) return r;
			final var arr  = this.onceInternal;
			final var size = this.onceInternalSize;
			for (var i = 0; i < size; i++) if (arr[i] == r) return r;
			if (size < arr.length) {
				arr[this.onceInternalSize++] = r;
				internalComponentKeys(r.components());
			}
			return r;
		}
		throw new IllegalStateException("metaOf("+clazz.getCanonicalName()+")=null");
	}

	private void pushState(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
		if (engineStack.depth + 1 >= maxDepth) throw new IllegalStateException();
		engineStack.push(type, obj, meta, targetIdx, targetType);
	}

	Object finalResult = null;

	private void applyValueToCurrentState(final Object value) {
		if (engineStack.depth < 0) return;
		final var c = engineStack.stack[engineStack.depth];
		if (c.targetIdx >= 0 || c.type != TYPE_OBJECT) {
			((ObjectMeta) c.meta).set(this, c.obj, c.targetIdx, value);
			if (c.type != TYPE_OBJECT) c.targetIdx++;
		}
	}

	private void applyValue(final Object result) throws IOException {
		applyValueToCurrentState(result);
		consumeCommaIfPresent();
	}

	private void popAndApply(final Object result) throws IOException {
		engineStack.stack[engineStack.depth].clean();
		engineStack.depth--;
		if (engineStack.depth >= 0) {
			applyValue(result);
		} else this.finalResult = result;
	}

	private void consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == (byte) ',') { pos++; return; }
		skipWhitespace();
		if (pos < limit && buffer[pos] == (byte) ',') pos++;
	}

	private Object runStateEngine(final byte startType, final Object startObj, final Object startMeta, final int startIdx, final Class<?> startTarget) throws Throwable {
		pushState(startType, startObj, startMeta, startIdx, startTarget);
		Class<?> targetType = null;
		finalResult = null;
		while (engineStack.depth >= 0) {
			if (pos < limit) {
				final var b = buffer[pos];
				if (b == 0x20 || b == 0x09 || b == 0x0A || b == 0x0D) skipWhitespace();
			} else skipWhitespace();
			final var c = engineStack.stack[engineStack.depth];
			final var shouldContinue = switch (c.type) {
			case TYPE_OBJECT           -> {
				if (peek() == (byte) '}') {
					read();
					popAndApply(((ObjectMeta) c.meta).end(this, c.obj));
					yield true;
				}
				final var meta = (ObjectMeta) c.meta;
				expect((byte) '"');
				c.targetIdx = parseStringKeyAsIndex(c.obj, (ObjectMeta) c.meta);
				if (pos < limit && buffer[pos] == (byte) ':') pos++;
				else { skipWhitespace(); expect((byte) ':'); }
				if (c.targetIdx < 0) {
					skipWhitespace();
					skipValue();
					consumeCommaIfPresent();
					yield true;
				}
				targetType = meta.type(c.targetIdx);
				yield false;
			}
			case TYPE_ARRAY, TYPE_COL  ->  {
				if (peek() == (byte) ']') {
					read();
					popAndApply(((ObjectMeta) c.meta).end(this, c.obj));
					yield true;
				}
				targetType = (c.type == TYPE_ARRAY ? c.targetType.componentType() : c.targetType);
				yield false;
			}
			default                    -> throw new IllegalStateException();
			};
			if (shouldContinue) continue;


			if (pos >= limit || buffer[pos] <= ' ') skipWhitespace();
			if (pos >= limit) throw new IllegalStateException("Unexpected EOF");
			final var b = buffer[pos];

			if (c.targetIdx >= 0 && c.meta instanceof final ObjectMeta meta) {
				switch(b) {
				case '-','0','1','2','3','4','5','6','7','8','9' -> {
					parseNumericPrimitive(meta, c.obj, c.targetIdx, targetType);
					if (c.type != TYPE_OBJECT) c.targetIdx++;
					consumeCommaIfPresent();
					continue;
				}
				case 'n' -> {
					parseNull();
					if (!engine.config().skipDefaultValues()) {
						if      (targetType == boolean.class)                             meta.set       (this, c.obj, c.targetIdx, Boolean.FALSE);
						else if (targetType == double.class || targetType == float.class) meta.setDouble (this, c.obj, c.targetIdx, 0.0);
						else if (targetType != null && targetType.isPrimitive())          meta.setLong   (this, c.obj, c.targetIdx, 0L);
						else                                                              meta.set       (this, c.obj, c.targetIdx, null);
					}
					if (c.type != TYPE_OBJECT) c.targetIdx++;
					consumeCommaIfPresent();
					continue;
				}
				case 't', 'f' -> {
					meta.set       (this, c.obj, c.targetIdx, parseBooleanPrimitive()); // there exists only two boolean Objects
					if (c.type != TYPE_OBJECT) c.targetIdx++;
					consumeCommaIfPresent();
					continue;
				}
				case '"' -> {
					pos++;
					meta.set(this, c.obj, c.targetIdx, parseStringValue());
					if (c.type != TYPE_OBJECT) c.targetIdx++;
					consumeCommaIfPresent();
					continue;
				}
				}
			}
			switch (b) {
			case '{' -> {
				pos++;
				final var t = targetType == null ? Map.class : targetType;
				final var meta = (t == Map.class) ? defaultMapMeta : metaOf(t);
				pushState(TYPE_OBJECT, meta.start(this), meta, -1, t);
			}
			case '[' -> {
				if (targetType == null) {
					pos++;
					if (genericCollectionMeta == null) genericCollectionMeta = new ObjectMeta(engine, Object.class, ObjectMeta.TYPE_COLLECTION);
					pushState(TYPE_COL, genericCollectionMeta.start(this), genericCollectionMeta, 0, Object.class);
				} else if (targetType.isArray() && targetType.componentType().isPrimitive()) {
					final var result = parsePrimitiveArray(targetType.componentType());
					final var parentState = engineStack.stack[engineStack.depth];
					((ObjectMeta) parentState.meta).set(this, parentState.obj, parentState.targetIdx, result);
					if (parentState.type != TYPE_OBJECT) parentState.targetIdx++;
					consumeCommaIfPresent();
				} else {
					pos++;
					final var isArray  = targetType.isArray();
					final var isSet    = Set.class.isAssignableFrom(targetType);
					final var metaType = isArray ? ObjectMeta.TYPE_OBJ_ARRAY : (isSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
					final var compType = isArray ? targetType.componentType() : (isSet ? Object.class : targetType);
					final var meta = getDynamicMeta(compType, metaType);
					pushState(isArray ? TYPE_ARRAY : TYPE_COL, meta.start(this), meta, 0, compType);
				}
			}
			default -> throw new IllegalStateException("Unexpected character: " + (char)b);
			}
		}
		return finalResult;
	}

	// ========================================================================
	// PUBLIC API
	// ========================================================================

	public <T> T readObject(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var meta = (targetClass == Map.class) ? defaultMapMeta : metaOf(targetClass);
		try {
			final var r = (engine.maxRecursiveDepth() <= 0
					? runStateEngine(TYPE_OBJECT, meta.start(this), meta, -1, targetClass)
							: parseRecordRecursive(meta, 0));
			return targetClass.cast(r);
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> T[] readRecords(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		final var meta = getDynamicMeta(targetClass, ObjectMeta.TYPE_OBJ_ARRAY);
		expect((byte) '[');
		try {
			return (T[]) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_ARRAY, meta.start(this), meta, 0, targetClass)
					: parseArrayRecursive(targetClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <V> Map<String, V> readMap(final Class<V> valueClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var meta = new ObjectMeta(engine, valueClass);
		try {
			return (Map<String, V>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_OBJECT, meta.start(this), meta, -1, valueClass)
					: parseRecordRecursive(meta, 0));

		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> readList(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var meta = getDynamicMeta(elementClass, ObjectMeta.TYPE_COLLECTION);
		try {
			return (List<T>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_COL, takeArray(10), meta, 0, elementClass)
					: parseArrayRecursive(elementClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> Set<T> readSet(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var meta = getDynamicMeta(elementClass, ObjectMeta.TYPE_SET);
		try {
			return (Set<T>)  (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_COL, meta.start(this), meta, 0, elementClass)
					: parseArrayRecursive(elementClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}
	// ========================================================================
	// RECURSIVE FAST-PATH ENGINE (Mit Hybrid-Fallback zur State-Machine)
	// ========================================================================

	private double[] parseDoubleArray(double[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new double[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private int[] parseIntArray   (int[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new int[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = (int) parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private long[] parseLongArray(long[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new long[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private boolean[] parseBooleanArray(boolean[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new boolean[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseBooleanPrimitive();
			consumeCommaIfPresent();
		}
	}

	private float[] parseFloatArray(float[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new float[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = (float) parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private Object parseDefaultArray(Object builder, final Class<?> compType) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == Array.getLength(builder)) {
				final var newArr = java.lang.reflect.Array.newInstance(compType, idx * 2);
				System.arraycopy(builder, 0, newArr, 0, idx);
				builder = newArr;
			}
			Array.set(builder, idx++, parsePrimitiveInline(buffer[pos], compType));
			consumeCommaIfPresent();
		}
	}

	private Object parsePrimitiveArray(final Class<?> compType) throws IOException {
		final var startSize = (lastArraySize > 1 && lastArraySize < 64) ? lastArraySize : 16;
		pos++;
		final var builder = Array.newInstance(compType, startSize);
		final Object arr;
		if      (compType == double .class) arr = parseDoubleArray ((double [])builder);
		else if (compType == long   .class) arr = parseLongArray   ((long   [])builder);
		else if (compType == int    .class) arr = parseIntArray    ((int    [])builder);
		else if (compType == boolean.class) arr = parseBooleanArray((boolean[])builder);
		else if (compType == float  .class) arr = parseFloatArray  ((float  [])builder);
		else                                arr = parseDefaultArray(           builder, compType);
		final var curSize = Array.getLength(arr);
		if(curSize != lastArraySize) {
			final var ret = Array.newInstance(compType, 16);
			System.arraycopy(arr, 0, ret, 0, lastArraySize);
			return ret;
		}
		lastArraySize = Array.getLength(arr);
		return arr;
	}

	private Object parseRecordRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		final var context = meta.start(this);
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) '}') { pos++; return meta.end(this, context); }
			expect((byte) '"');
			final var targetIdx = parseStringKeyAsIndex(context, meta);
			if (pos < limit && buffer[pos] == (byte) ':') pos++;
			else { skipWhitespace(); expect((byte) ':'); }
			if (targetIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); continue; }
			final var type = meta.type(targetIdx);
			skipWhitespace();
			final Object value;
			final var b = buffer[pos];
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, targetIdx, type);
			case 'n' -> {
				parseNull();
				if (!engine.config().skipDefaultValues()) {
					if      (type == boolean.class)                       meta.set       (this, context, targetIdx, false);
					else if (type == double.class || type == float.class) meta.setDouble (this, context, targetIdx, 0.0);
					else if (type != null && type.isPrimitive())          meta.setLong   (this, context, targetIdx, 0L);
					else                                                  meta.set       (this, context, targetIdx, null);
				}
			}
			case 't' -> {
				parseTruePrimitive();
				meta.set(this, context, targetIdx, Boolean.TRUE);
			}
			case 'f' -> {
				parseFalsePrimitive();
				meta.set(this, context, targetIdx, Boolean.FALSE);
			}
			case '"' -> {
				pos++;
				meta.set(this, context, targetIdx, parseStringValue());
			}
			case '{' -> {
				if (recursionDeep >= this.maxDepth) {
					pos++;
					final var t = type == null ? Map.class : type;
					final var fallbackMeta = t == Map.class ? defaultMapMeta : metaOf(t);
					value = runStateEngine(TYPE_OBJECT, fallbackMeta.start(this), fallbackMeta, -1, t);
				} else {
					pos++;
					value = parseRecordRecursive(type == null || type == Map.class ? defaultMapMeta : metaOf(type), recursionDeep + 1);
				}
				meta.set(this, context, targetIdx, value);
			}
			case '[' -> {
				if (type == null) {
					pos++;
					if (recursionDeep >= this.maxDepth) {
						if (genericCollectionMeta == null) genericCollectionMeta = new ObjectMeta(engine, Object.class, ObjectMeta.TYPE_COLLECTION);
						value = runStateEngine(TYPE_COL, genericCollectionMeta.start(this), genericCollectionMeta, 0, Object.class);
					} else {
						value = parseArrayRecursive(null, recursionDeep + 1);
					}
				} else if (recursionDeep >= this.maxDepth) {
					pos++;
					if (type.isArray() && type.componentType().isPrimitive()) {
						value = parsePrimitiveArray(type.componentType());
					} else {
						final var isArray  = type.isArray();
						final var isSet    = Set.class.isAssignableFrom(type);
						final var metaType = isArray ? ObjectMeta.TYPE_OBJ_ARRAY : (isSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
						final var compType = isArray ? type.componentType() : (isSet ? Object.class : type);
						final var subMeta = getDynamicMeta(compType, metaType);
						value = runStateEngine(isArray ? TYPE_ARRAY : TYPE_COL, subMeta.start(this), subMeta, 0, compType);
					}
				} else {
					pos++;
					value = parseArrayRecursive(type, recursionDeep + 1);
				}
				meta.set(this, context, targetIdx, value);
			}
			default -> throw new IllegalStateException(); // NOT number/boolean/string/array/object
			}
			consumeCommaIfPresent();
		}
	}

	private Object parsePrimitiveInline(final byte b, final Class<?> type) throws IOException {
		return switch (b) {
		case '"' -> {
			pos++;
			yield parseStringValue();
		}
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(type);
		};
	}

	@Override public int depth() { return depth; }

	private Object parseArrayRecursive(final Class<?> targetType, final int recursionDeep) throws Throwable {
		depth = recursionDeep;
		final ObjectMeta meta;
		final Class<?> compType;

		if (targetType == null) {
			if (genericCollectionMeta == null) genericCollectionMeta = new ObjectMeta(engine, Object.class, ObjectMeta.TYPE_COLLECTION);
			meta = genericCollectionMeta;
			compType = Object.class;
		} else if (targetType.isArray()) {
			compType = targetType.componentType();
			if (compType.isPrimitive()) return parsePrimitiveArray(compType);
			meta = getDynamicMeta(compType, ObjectMeta.TYPE_OBJ_ARRAY);
		} else {
			final var isSet = Set.class.isAssignableFrom(targetType);
			compType = isSet ? Object.class : targetType;
			meta = getDynamicMeta(compType, isSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
		}

		final var context = meta.start(this);
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') {
				pos++;
				return meta.end(this, context);
			}

			final var b = buffer[pos];
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, idx++, compType);
			case 'n' -> {
				parseNull();
				if (!engine.config().skipDefaultValues()) {
					if      (compType == boolean.class)                             meta.set       (this, context, idx, false);
					else if (compType == double.class || compType == float.class)   meta.setDouble (this, context, idx, 0.0);
					else if (compType != null && compType.isPrimitive())            meta.setLong   (this, context, idx, 0L);
					else                                                            meta.set       (this, context, idx, null);
				}
				idx++;
			}
			case 't','f' -> meta.set(this, context, idx++, parseBooleanPrimitive());
			case '"' -> {
				pos++;
				meta.set(this, context, idx++, parseStringValue());
			}
			default -> {
				final Object val;
				if (recursionDeep >= this.maxDepth) {
					if (b == '{') {
						pos++;
						final var t = compType == Object.class ? Map.class : compType;
						final var fallbackMeta = t == Map.class ? defaultMapMeta : metaOf(t);
						val = runStateEngine(TYPE_OBJECT, fallbackMeta.start(this), fallbackMeta, -1, t);
					} else if (b == '[') {
						if (compType == Object.class) {
							pos++;
							if (genericCollectionMeta == null) genericCollectionMeta = new ObjectMeta(engine, Object.class, ObjectMeta.TYPE_COLLECTION);
							val = runStateEngine(TYPE_COL, genericCollectionMeta.start(this), genericCollectionMeta, 0, Object.class);
						} else {
							pos++;
							if (compType.isArray() && compType.componentType().isPrimitive()) {
								val = parsePrimitiveArray(compType.componentType());
							} else {
								final var isSubArray = compType.isArray();
								final var isSubSet   = Set.class.isAssignableFrom(compType);
								final var subMetaType = isSubArray ? ObjectMeta.TYPE_OBJ_ARRAY : (isSubSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
								final var subCompType = isSubArray ? compType.componentType() : (isSubSet ? Object.class : compType);
								final var subMeta = getDynamicMeta(subCompType, subMetaType);
								val = runStateEngine(isSubArray ? TYPE_ARRAY : TYPE_COL, subMeta.start(this), subMeta, 0, subCompType);
							}
						}
					} else {
						val = parsePrimitiveInline(b, compType);
					}
				} else // Reguläre Rekursion
					if (b == '{') {
						pos++;
						val = parseRecordRecursive(compType == Object.class ? defaultMapMeta : metaOf(compType), recursionDeep + 1);
					} else if (b == '[') {
						pos++;
						val = parseArrayRecursive(compType == Object.class ? null : compType, recursionDeep + 1);
						depth = recursionDeep;
					} else {
						val = parsePrimitiveInline(b, compType);

					}
				meta.set(this, context, idx++, val);
			}
			}
			consumeCommaIfPresent();
		}
	}
}