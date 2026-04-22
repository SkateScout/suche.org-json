package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final byte TYPE_OBJECT = 1;
	private static final byte TYPE_ARRAY  = 2;
	private static final byte TYPE_COL    = 3;
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);
	private static final ObjectPool<CtxStack>        STACK_POOL  = new ObjectPool<>(64, CtxStack::new);

	static final class CTX {
		byte     type;
		Object   obj;
		Object   meta;
		int      targetIdx;
		Class<?> targetType;

		void set(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
			this.type       = type;
			this.obj        = obj;
			this.meta       = meta;
			this.targetIdx  = targetIdx;
			this.targetType = targetType;
		}

		void clear() {
			this.obj        = null;
			this.meta       = null;
			this.targetType = null;
		}
	}

	static final class CtxStack {
		CTX[] stack;
		int depth = -1;

		CtxStack() {
			stack = new CTX[64];
			for (var i = 0; i < 64; i++) stack[i] = new CTX();
		}

		void push(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(type, obj, meta, targetIdx, targetType);
		}

		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clear();
			depth = -1;
		}
	}

	private ObjectMeta defaultMapMeta;
	private CtxStack stack;

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.stack = STACK_POOL.acquire();
		s.stack.depth = -1;
		s.defaultMapMeta = engine.metaOf(Map.class);
		return s;
	}

	@Override public void close() throws IOException {
		if (stack != null) {
			stack.clear();
			STACK_POOL.release(stack);
			stack = null;
		}
		depth = -1;
		if (internPool != null) internPool.clear();
		onceInternal.clear();
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	ObjectMeta metaOf(final Class<?> clazz) {
		if(engine.metaOf(clazz) instanceof final ObjectMeta r) {
			if(r .components() == null || r.components().length == 0) return r;
			if(onceInternal.add(r))
				for(final var e : r.components()) {
					final var src = e.name().getBytes(StandardCharsets.UTF_8);
					var hash = 0;
					var isAscii = true;
					for (final byte b : src) {
						hash = 31 * hash + b;
						if(b<0) isAscii = false;
					}
					internBytes(src, 0, src.length, hash, isAscii);
				}
			return r;
		}
		throw new IllegalStateException("metaOf("+clazz.getCanonicalName()+")=null");
	}

	private void pushState(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
		if (stack.depth + 1 >= maxDepth) throw new IllegalStateException();
		stack.push(type, obj, meta, targetIdx, targetType);
	}


	Object finalResult = null;

	private void applyValueToCurrentState(final Object value) {
		if (stack.depth < 0) return;
		final var c = stack.stack[stack.depth];
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
		stack.stack[stack.depth].clear();
		stack.depth--;
		if (stack.depth >= 0) {
			applyValue(result);
		} else {
			this.finalResult = result;
		}
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
		while (stack.depth >= 0) {
			if (pos < limit) {
				final var b = buffer[pos];
				if (b == 0x20 || b == 0x09 || b == 0x0A || b == 0x0D) skipWhitespace();
			} else skipWhitespace();
			final var c = stack.stack[stack.depth];
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
						if      (targetType == boolean.class)                             meta.setBoolean(this, c.obj, c.targetIdx, false);
						else if (targetType == double.class || targetType == float.class) meta.setDouble (this, c.obj, c.targetIdx, 0.0);
						else if (targetType != null && targetType.isPrimitive())          meta.setLong   (this, c.obj, c.targetIdx, 0L);
						else                                                              meta.set       (this, c.obj, c.targetIdx, null);
					}
					if (c.type != TYPE_OBJECT) c.targetIdx++;
					consumeCommaIfPresent();
					continue;
				}
				case 't', 'f' -> {
					final var val = parseBooleanPrimitive();
					if (targetType == boolean.class) meta.setBoolean(this, c.obj, c.targetIdx, val);
					else                             meta.set       (this, c.obj, c.targetIdx, Boolean.valueOf(val));
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
				if (targetType != null && targetType.isArray() && targetType.componentType().isPrimitive()) {
					final var result = parsePrimitiveArray(targetType.componentType());
					final var parentState = stack.stack[stack.depth];
					((ObjectMeta) parentState.meta).set(this, parentState.obj, parentState.targetIdx, result);
					if (parentState.type != TYPE_OBJECT) parentState.targetIdx++;
					consumeCommaIfPresent();
				} else {
					pos++;
					final var isArray  = targetType != null && targetType.isArray();
					final var isSet    = targetType != null && Set.class.isAssignableFrom(targetType);
					final var metaType = isArray ? ObjectMeta.TYPE_OBJ_ARRAY : (isSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
					final var compType = isArray ? targetType.componentType() : (targetType != null && !isSet ? targetType : Object.class);
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

	private Object parsePrimitiveArray(final Class<?> compType) throws IOException {
		pos++;
		if (compType == double.class) {
			var builder = new double[16];
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') { pos++; return java.util.Arrays.copyOf(builder, idx); }
				if (idx == builder.length) {
					final var newArr = new double[builder.length * 2];
					System.arraycopy(builder, 0, newArr, 0, builder.length);
					builder = newArr;
				}
				builder[idx++] = parseDoublePrimitive();
				consumeCommaIfPresent();
			}
		} else if (compType == int.class) {
			var builder = new int[16];
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') { pos++; return java.util.Arrays.copyOf(builder, idx); }
				if (idx == builder.length) {
					final var newArr = new int[builder.length * 2];
					System.arraycopy(builder, 0, newArr, 0, builder.length);
					builder = newArr;
				}
				builder[idx++] = (int) parseLongPrimitive();
				consumeCommaIfPresent();
			}
		} else if (compType == long.class) {
			var builder = new long[16];
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') { pos++; return java.util.Arrays.copyOf(builder, idx); }
				if (idx == builder.length) {
					final var newArr = new long[builder.length * 2];
					System.arraycopy(builder, 0, newArr, 0, builder.length);
					builder = newArr;
				}
				builder[idx++] = parseLongPrimitive();
				consumeCommaIfPresent();
			}
		} else if (compType == boolean.class) {
			var builder = new boolean[16];
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') { pos++; return java.util.Arrays.copyOf(builder, idx); }
				if (idx == builder.length) {
					final var newArr = new boolean[builder.length * 2];
					System.arraycopy(builder, 0, newArr, 0, builder.length);
					builder = newArr;
				}
				builder[idx++] = parseBooleanPrimitive();
				consumeCommaIfPresent();
			}
		} else if (compType == float.class) {
			var builder = new float[16];
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') { pos++; return java.util.Arrays.copyOf(builder, idx); }
				if (idx == builder.length) {
					final var newArr = new float[builder.length * 2];
					System.arraycopy(builder, 0, newArr, 0, builder.length);
					builder = newArr;
				}
				builder[idx++] = (float) parseDoublePrimitive();
				consumeCommaIfPresent();
			}
		} else {
			var builder = java.lang.reflect.Array.newInstance(compType, 16);
			var idx = 0;
			while (true) {
				skipWhitespace();
				if (pos < limit && buffer[pos] == (byte) ']') {
					pos++;
					final var finalArray = java.lang.reflect.Array.newInstance(compType, idx);
					System.arraycopy(builder, 0, finalArray, 0, idx);
					return finalArray;
				}
				if (idx == Array.getLength(builder)) {
					final var newArr = java.lang.reflect.Array.newInstance(compType, idx * 2);
					System.arraycopy(builder, 0, newArr, 0, idx);
					builder = newArr;
				}
				Array.set(builder, idx++, parsePrimitiveInline(buffer[pos], compType));
				consumeCommaIfPresent();
			}
		}
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
					if      (type == boolean.class)                       meta.setBoolean(this, context, targetIdx, false);
					else if (type == double.class || type == float.class) meta.setDouble (this, context, targetIdx, 0.0);
					else if (type != null && type.isPrimitive())          meta.setLong   (this, context, targetIdx, 0L);
					else                                                  meta.set       (this, context, targetIdx, null);
				}
			}
			case 't' -> {
				parseTruePrimitive();
				if (type == boolean.class) meta.setBoolean(this, context, targetIdx, true);
				else                       meta.set       (this, context, targetIdx, Boolean.TRUE);
			}
			case 'f' -> {
				parseFalsePrimitive();
				if (type == boolean.class) meta.setBoolean(this, context, targetIdx, false);
				else                       meta.set       (this, context, targetIdx, Boolean.FALSE);
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
				if (recursionDeep >= this.maxDepth) {
					pos++;
					if (type != null && type.isArray() && type.componentType().isPrimitive()) {
						value = parsePrimitiveArray(type.componentType());
					} else {

						final var isArray  = type != null && type.isArray();
						final var isSet    = type != null && Set.class.isAssignableFrom(type);
						final var metaType = isArray ? ObjectMeta.TYPE_OBJ_ARRAY : (isSet ? ObjectMeta.TYPE_SET : ObjectMeta.TYPE_COLLECTION);
						final var compType = isArray ? type.componentType() : (type != null && !isSet ? type : Object.class);
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
		case '"' -> { pos++; yield parseStringValue(); }
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(type);
		};
	}

	private Object parseArrayRecursive(final Class<?> targetType, final int recursionDeep) throws Throwable {
		final var isArray = targetType != null && targetType.isArray();
		final var compType = isArray ? targetType.componentType() : (targetType != null ? targetType : Object.class);
		if (isArray && compType.isPrimitive()) return parsePrimitiveArray(compType);
		final var meta = getDynamicMeta(compType, ObjectMeta.TYPE_OBJ_ARRAY);
		final var context = meta.start(this);
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') {
				pos++;
				return meta.end(this, context); // Elegante Rückgabe über Meta!
			}

			final var b = buffer[pos];
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, idx++, compType);
			case 'n' -> {
				parseNull();
				if (!engine.config().skipDefaultValues()) {
					if      (compType == boolean.class)                             meta.setBoolean(this, context, idx, false);
					else if (compType == double.class || compType == float.class)   meta.setDouble (this, context, idx, 0.0);
					else if (compType != null && compType.isPrimitive())            meta.setLong   (this, context, idx, 0L);
					else                                                            meta.set       (this, context, idx, null);
				}
				idx++;
			}
			case 't','f' -> {
				final var val = parseBooleanPrimitive();
				if (compType == boolean.class) meta.setBoolean(this, context, idx, val);
				else                           meta.set       (this, context, idx, Boolean.valueOf(val));
				idx++;
			}
			case '"' -> {
				pos++;
				meta.set(this, context, idx++, parseStringValue());
			}
			default -> {
				final Object val;
				if (recursionDeep >= this.maxDepth) {
					if (b == '{') {
						pos++;
						final var t = compType == null ? Map.class : compType;
						final var fallbackMeta = t == Map.class ? defaultMapMeta : metaOf(t);
						val = runStateEngine(TYPE_OBJECT, fallbackMeta.start(this), fallbackMeta, -1, t);
					} else if (b == '[') {
						pos++;
						if (compType != null && compType.isArray() && compType.componentType().isPrimitive()) {
							val = parsePrimitiveArray(compType.componentType());
						} else {
							final var isSubArray = compType != null && compType.isArray();
							final var subCompType = isSubArray ? compType.componentType() : (compType != null ? compType : Object.class);
							final var subMeta = getDynamicMeta(subCompType, ObjectMeta.TYPE_OBJ_ARRAY);
							val = runStateEngine(isSubArray ? TYPE_ARRAY : TYPE_COL, subMeta.start(this), subMeta, 0, subCompType);
						}
					} else {
						val = parsePrimitiveInline(b, compType);
					}
				} else // Reguläre Rekursion
					if (b == '{') {
						pos++;
						val = parseRecordRecursive(compType == null || compType == Map.class ? defaultMapMeta : metaOf(compType), recursionDeep + 1);
					} else if (b == '[') {
						pos++;
						val = parseArrayRecursive(compType, recursionDeep + 1);
					} else {
						val = parsePrimitiveInline(b, compType);
					}
				meta.set(this, context, idx++, val);
				break;
			}
			}
			consumeCommaIfPresent();
		}
	}
}