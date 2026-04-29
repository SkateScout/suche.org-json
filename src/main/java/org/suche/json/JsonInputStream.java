package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);

	private final CtxStack engineStack = new CtxStack();
	private int lastArraySize = 16;
	ObjectMeta[] metaCache;

	static final class CTX {
		long typeDesc;
		Object obj;
		Object meta;
		int targetIdx;
		void set(final long pTypeDesc, final Object pObj, final Object pMeta, final int pTargetIdx) { this.typeDesc = pTypeDesc; this.obj = pObj; this.meta = pMeta; this.targetIdx = pTargetIdx; }
		public void clean() { this.obj = null; this.meta = null; }
	}

	static final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack = stack64;
		int depth = -1;
		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }
		void push(final long typeDesc, final Object obj, final Object meta, final int targetIdx) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(typeDesc, obj, meta, targetIdx);
		}
		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clean();
			depth = -1;
		}
	}

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.engineStack.depth = -1;
		s.lastArraySize = 16;
		s.metaCache = engine.metaCache();
		return s;
	}

	@Override
	public void close() throws IOException {
		engineStack.clear();
		depth = -1;
		if (internPool != null) internPool.clear();
		Arrays.fill(onceInternal, 0, onceInternalSize, null);
		onceInternalSize = 0;
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	private void fillNullValue(final long typeDesc, final Object obj, final int targetIdx, final ObjectMeta meta) throws IOException {
		parseNull();
		if (engine.config().skipDefaultValues() && typeDesc >= 0L) return;
		if ((typeDesc & 1L) != 0L) {
			final var primId = (int) (typeDesc >> 1);
			switch (primId) {
			case ObjectMeta.PRIM_BOOLEAN                       -> meta.set(this, obj, targetIdx, Boolean.FALSE);
			case ObjectMeta.PRIM_DOUBLE, ObjectMeta.PRIM_FLOAT -> meta.setDouble(this, obj, targetIdx, 0.0);
			case ObjectMeta.PRIM_INT, ObjectMeta.PRIM_LONG     -> meta.setLong(this, obj, targetIdx, 0L);
			default -> meta.set(this, obj, targetIdx, null);
			}
		} else {
			meta.set(this, obj, targetIdx, null);
		}
	}

	private Object runStateEngine(final long startTypeDesc, final Object startObj, final Object startMeta, final int startIdx) throws Throwable {
		var curTypeDesc = startTypeDesc;
		var curObj = startObj;
		var curMeta = (ObjectMeta) startMeta;
		var curIdx = startIdx;
		engineStack.clear();
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			var b = buffer[pos];
			switch (b) {
			case '\t', '\n', '\r', ' ' -> { for(pos++; pos < limit; pos++) if (((b = buffer[pos]) & 0xC0) != 0 || ((1L << b) & WHITESPACE_MASK) == 0) break; }
			case '"' -> {
				if (curTypeDesc >= 0L && curIdx < 0) {
					curIdx = parseStringKeyAsIndex(curObj, curMeta);
					expect((byte) ':');
				} else {
					curMeta.set(this, curObj, curIdx, parseStringValue());
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				}
			}
			case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
				// TODO why is here no curTypeDesc type ?
				parseNumericPrimitive(curMeta, curObj, curIdx, null);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 'n' -> {
				fillNullValue(curTypeDesc, curObj, curIdx, curMeta);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 't', 'f' -> { // Combined to reduce method sice
				curMeta.set(this, curObj, curIdx, parseTrueOrFalse(b == 't'));
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '{' -> {
				engineStack.push(curTypeDesc, curObj, curMeta, curIdx);	// No issue if later an exception came
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				curTypeDesc = curMeta.fieldDescriptor(curIdx);
				pos++;
				if (curTypeDesc < 0L || (curTypeDesc & 1L) != 0L) throw new IllegalStateException("Expected got unexpected '{'");
				curMeta = metaCache[(int) (curTypeDesc >> 1)];
				curObj = curMeta.start(this);
				curIdx = -1;
			}
			case '}', ']' -> {
				pos++;
				// IMPORTANT: Must be evaluated BEFORE parent overwrites curMeta/curObj
				final var finishedObj = curMeta.end(this, curObj);
				if (engineStack.depth < 0) return finishedObj;
				final var parent = engineStack.stack[engineStack.depth--];
				curTypeDesc = parent.typeDesc;
				curObj      = parent.obj;
				curMeta     = (ObjectMeta) parent.meta;
				curIdx      = parent.targetIdx;
				curMeta.set(this, curObj, curIdx, finishedObj);
				curIdx      = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '[' -> {
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				pos++;
				var childDesc = curMeta.fieldDescriptor(curIdx);
				if (childDesc >= 0L) {
					if (((int) (childDesc >> 1))!= ObjectMeta.IDX_MAP) throw new IllegalStateException("Expected object, got '['");
					childDesc = ObjectMeta.DESC_COLLECTION;
				}
				if ((childDesc & 1L) != 0L) {
					curMeta.set(this, curObj, curIdx, parsePrimitiveArray(childDesc));
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				} else {
					engineStack.push(curTypeDesc, curObj, curMeta, curIdx);
					curTypeDesc = childDesc;
					curMeta = metaCache[(int) (curTypeDesc >> 1)];
					curObj = curMeta.start(this);
					curIdx = 0;
				}
			}
			default -> throw new IllegalStateException();
			}
		}
	}

	private void consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == ',') { pos++; return; }
		consumeCommaSlow();
	}

	private void consumeCommaSlow() throws IOException {
		var p = pos;
		final var l = limit;
		final var buf = buffer;
		while (p < l) {
			final var c = buf[p];
			if (c == ',') { pos = p + 1; return; }
			if ((c & 0xC0) != 0 || ((1L << c) & WHITESPACE_MASK) == 0) { pos = p; return; }
			p++;
		}
		pos = p;
		skipWhitespace();
		if (pos < limit && buffer[pos] == ',') pos++;
	}

	private Object parsePrimitiveArray(final long typeDesc) throws IOException {
		final var primId = (int)(typeDesc>>1);
		final var size = (lastArraySize > 0 && lastArraySize < 1024) ? lastArraySize : 16;
		return switch (primId) {
		case ObjectMeta.PRIM_DOUBLE  -> parseDoubleArray(new double[size]);
		case ObjectMeta.PRIM_LONG    -> parseLongArray(new long[size]);
		case ObjectMeta.PRIM_INT     -> parseIntArray(new int[size]);
		case ObjectMeta.PRIM_BOOLEAN -> parseBooleanArray(new boolean[size]);
		default -> null; // Fallback für char/byte/short
		};
	}

	private double[] parseDoubleArray(double[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private int[] parseIntArray(int[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = (int) parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private long[] parseLongArray(long[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private boolean[] parseBooleanArray(boolean[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseBooleanPrimitive();
			consumeCommaIfPresent();
		}
	}

	@Override public int depth() { return engineStack.depth; }

	// ############################### Public Part #############################################

	public Object readObject(final Class<?> targetType) throws Throwable {
		skipWhitespace();
		if (pos >= limit) return null;
		final var b = buffer[pos];
		var targetMeta = engine.metaOf(targetType);

		// FIX: Wenn targetType Object.class ist, liefert metaOf() die IDX_MAP.
		// Wenn wir aber ein Array lesen, MÜSSEN wir auf IDX_COLLECTION umschalten!
		if (b == '[' && targetMeta != null && (targetMeta.cacheIndex == ObjectMeta.IDX_MAP || targetMeta.cacheIndex == ObjectMeta.IDX_GENERIC)) {
			targetMeta = metaCache[ObjectMeta.IDX_COLLECTION];
		}

		final var isArray = (b == '[');
		final var startTypeDesc = EngineImpl.createTypeDesc(isArray, false, targetMeta != null ? targetMeta.cacheIndex : 0);

		if (this.maxRecursiveDepth <= 0) {
			if (b == '{') pos++;
			return runStateEngine(startTypeDesc, targetMeta != null ? targetMeta.start(this) : null, targetMeta, isArray ? 0 : -1);
		}
		if (b == '{') {
			pos++;
			return parseRecordRecursive(targetMeta != null ? targetMeta : metaCache[ObjectMeta.IDX_MAP], 0);
		}
		if (b == '[') {
			pos++;
			return parseArrayRecursive(targetMeta != null ? targetMeta : metaCache[ObjectMeta.IDX_COLLECTION], 0);
		}
		return parsePrimitiveInline(b, targetType);
	}


	@SuppressWarnings("unchecked")
	public <T> T[] readRecords(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var metaIdx = ((EngineImpl) engine).getDynamicMetaId(targetClass, ObjectMeta.TYPE_OBJ_ARRAY);
		final var meta = metaCache[metaIdx];
		final var typeDesc = EngineImpl.createTypeDesc(true, false, metaIdx);

		try {
			return (T[]) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <V> Map<String, V> readMap(final Class<V> valueClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var metaIdx = ((EngineImpl) engine).getDynamicMetaId(valueClass, ObjectMeta.TYPE_MAP);
		final var meta = metaCache[metaIdx];
		final var typeDesc = EngineImpl.createTypeDesc(false, false, metaIdx);

		try {
			return (Map<String, V>) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, -1)
					: parseRecordRecursive(meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> readList(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var metaIdx = ((EngineImpl) engine).getDynamicMetaId(elementClass, ObjectMeta.TYPE_COLLECTION);
		final var meta = metaCache[metaIdx];
		final var typeDesc = EngineImpl.createTypeDesc(true, false, metaIdx);

		try {
			return (List<T>) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> Set<T> readSet(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var metaIdx = ((EngineImpl) engine).getDynamicMetaId(elementClass, ObjectMeta.TYPE_SET);
		final var meta = metaCache[metaIdx];
		final var typeDesc = EngineImpl.createTypeDesc(true, false, metaIdx);

		try {
			return (Set<T>)  (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	// ========================================================================
	// RECURSIVE FAST-PATH ENGINE (Mit Hybrid-Fallback zur State-Machine)
	// ========================================================================

	private Object parsePrimitiveInline(final byte b, final Class<?> type) throws IOException {
		return switch (b) {
		case '"' -> parseStringValue();
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(type);
		};
	}

	private Object parseRecordRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		final var context = meta.start(this);
		final var limitDepth = this.maxDepth;
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			int targetIdx;
			var b = buffer[pos];
			if ((b & 0xC0) == 0 && ((1L << b) & WHITESPACE_MASK) != 0) skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) '}') { pos++; return meta.end(this, context); }
			targetIdx = parseStringKeyAsIndex(context, meta);
			expect((byte) ':');
			if (targetIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); continue; }
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			b = buffer[pos];
			if ((b & 0xC0) == 0 && ((1L << b) & WHITESPACE_MASK) != 0) {
				skipWhitespace();
				b = buffer[pos];
			}
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, targetIdx, null);
			case 'n' -> fillNullValue(meta.fieldDescriptor(targetIdx), context, targetIdx, meta);
			case 't' -> meta.set(this, context, targetIdx, parseTrue());
			case 'f' -> meta.set(this, context, targetIdx, parseFalse());
			case '"' -> meta.set(this, context, targetIdx, parseStringValue());
			case '{' -> {
				final var fieldDesc = meta.fieldDescriptor(targetIdx);
				if (fieldDesc < 0L || (fieldDesc & 1L) != 0L) throw new IllegalStateException("Expected array, got '{'");
				final var childMeta = meta.childMeta(targetIdx);
				final var fallbackMeta = childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_MAP];
				if (recursionDeep >= limitDepth) {
					final var passDesc = fieldDesc == 0L ? (((long) ObjectMeta.IDX_MAP) << 1) : fieldDesc;
					meta.set(this, context, targetIdx, runStateEngine(passDesc, fallbackMeta.start(this), fallbackMeta, -1));
				} else {
					pos++;
					meta.set(this, context, targetIdx, parseRecordRecursive(fallbackMeta, recursionDeep + 1));
				}
			}
			case '[' -> {
				final var fieldDesc = meta.fieldDescriptor(targetIdx);
				final var metaIdx = (int) (fieldDesc >>> 1);
				if (fieldDesc >= 0L && metaIdx != ObjectMeta.IDX_GENERIC && metaIdx != ObjectMeta.IDX_MAP) throw new IllegalStateException("Expected object, got '['");
				if ((fieldDesc & 1L) != 0L) {
					pos++;
					meta.set(this, context, targetIdx, parsePrimitiveArray(fieldDesc));
				} else {
					final var childMeta = meta.childMeta(targetIdx);
					final var isFallbackToCollection = (fieldDesc >= 0L) && (metaIdx == ObjectMeta.IDX_MAP || metaIdx == ObjectMeta.IDX_GENERIC);
					final var fallbackMeta = isFallbackToCollection ? metaCache[ObjectMeta.IDX_COLLECTION] : (childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_COLLECTION]);
					if (recursionDeep >= limitDepth) {
						final var passDesc = isFallbackToCollection ? ObjectMeta.DESC_COLLECTION : (fieldDesc == 0L ? ObjectMeta.DESC_COLLECTION : fieldDesc);
						meta.set(this, context, targetIdx, runStateEngine(passDesc, fallbackMeta.start(this), fallbackMeta, 0));
					} else {
						pos++;
						meta.set(this, context, targetIdx, parseArrayRecursive(fallbackMeta, recursionDeep + 1));
					}
				}
			}
			default -> throw new IllegalStateException();
			}
			if (pos < limit && buffer[pos] == ',') pos++;  else consumeCommaSlow();
		}
	}

	private Object parseArrayRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		depth = recursionDeep;
		final var context    = meta.start(this);
		final var limitDepth = this.maxDepth;
		final var childDesc  = meta.fieldDescriptor(0);
		var idx = 0;
		byte state = 0;
		// LAZY EVALUATION VARIABLES (Werden nur berechnet, wenn zwingend nötig)
		long objPassDesc = 0, arrPassDesc = 0;
		ObjectMeta objFallbackMeta = null, arrFallbackMeta = null;
		var isPrimitive = false;
		var metaIdx = 0;
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			switch (buffer[pos]) {
			case '\t','\n','\r',' ' -> { pos++; }							// SKIP   Whitespace
			case ']' -> { pos++; return meta.end(this, context); }	// ALWAYS Possible
			case '-','0','1','2','3','4','5','6','7','8','9' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of NUMERIC expected");
				state = 1;
				parseNumericPrimitive(meta, context, idx, null);
			}
			case ',' -> {
				idx++;
				if(state==0) throw new IllegalStateException("VALUE instead of KOMMA expected");
				state = 0;
				pos++;
			}
			case 'n' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of NULL expected");
				state = 1;
				fillNullValue(childDesc, context, idx, meta);
			}
			case 't' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of BOOLEAN expected");
				state = 1;
				meta.set(this, context, idx, parseTrue());
			}
			case 'f' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of BOOLEAN expected");
				state = 1;
				meta.set(this, context, idx, parseFalse());
			}
			case '"' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of STRING expected");
				state = 1;
				meta.set(this, context, idx, parseStringValue());
			}
			case '{' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of OBJECT expected");
				state = 1;
				if (objFallbackMeta == null) {	// LAZY INIT
					isPrimitive = (childDesc & 1L) != 0L;
					metaIdx = (int) (childDesc >>> 1);
					final var childMeta = metaIdx == ObjectMeta.IDX_GENERIC ? null : metaCache[metaIdx];
					objFallbackMeta = childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_MAP];
					objPassDesc = childDesc == 0L ? (((long) ObjectMeta.IDX_MAP) << 1) : childDesc;
					final var isFallbackToCollection = ((childDesc >= 0L) && (metaIdx == ObjectMeta.IDX_MAP || metaIdx == ObjectMeta.IDX_GENERIC));
					arrFallbackMeta = isFallbackToCollection ? metaCache[ObjectMeta.IDX_COLLECTION] : (childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_COLLECTION]);
					arrPassDesc = isFallbackToCollection ? ObjectMeta.DESC_COLLECTION : (childDesc == 0L ? ObjectMeta.DESC_COLLECTION : childDesc);
				}

				if (childDesc < 0L || isPrimitive) throw new IllegalStateException("Expected array, got '{'");
				if (recursionDeep >= limitDepth) {
					meta.set(this, context, idx, runStateEngine(objPassDesc, objFallbackMeta.start(this), objFallbackMeta, -1));
				} else {
					pos++;
					meta.set(this, context, idx, parseRecordRecursive(objFallbackMeta, recursionDeep + 1));
				}
			}
			case '[' -> {
				if(state==1) throw new IllegalStateException("Komma or Closeing breaket instead of ARRAY expected");
				state = 1;
				if (arrFallbackMeta == null) {	// LAZY INIT
					isPrimitive = (childDesc & 1L) != 0L;
					metaIdx = (int) (childDesc >>> 1);
					final var childMeta = metaIdx == ObjectMeta.IDX_GENERIC ? null : metaCache[metaIdx];
					objFallbackMeta = childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_MAP];
					objPassDesc = childDesc == 0L ? (((long) ObjectMeta.IDX_MAP) << 1) : childDesc;
					final var isFallbackToCollection = ((childDesc >= 0L) && (metaIdx == ObjectMeta.IDX_MAP || metaIdx == ObjectMeta.IDX_GENERIC));
					arrFallbackMeta = isFallbackToCollection ? metaCache[ObjectMeta.IDX_COLLECTION] : (childMeta != null ? childMeta : metaCache[ObjectMeta.IDX_COLLECTION]);
					arrPassDesc     = isFallbackToCollection ?           ObjectMeta.DESC_COLLECTION : (childDesc == 0L ? ObjectMeta.DESC_COLLECTION : childDesc);
				}

				if ((childDesc >= 0L) && metaIdx != ObjectMeta.IDX_GENERIC && metaIdx != ObjectMeta.IDX_MAP) throw new IllegalStateException("Expected object, got '['");
				if (isPrimitive) {
					pos++;
					meta.set(this, context, idx, parsePrimitiveArray(childDesc));
				} else if (recursionDeep >= limitDepth) {
					meta.set(this, context, idx, runStateEngine(arrPassDesc, arrFallbackMeta.start(this), arrFallbackMeta, 0));
				} else {
					pos++;
					meta.set(this, context, idx, parseArrayRecursive(arrFallbackMeta, recursionDeep + 1));
				}
			}
			default -> throw new IllegalStateException("Unexpected byte["+buffer[pos]+"] instead of "+(state==0?"VALUE":"KOMMA/END"));
			}
		}
	}

}