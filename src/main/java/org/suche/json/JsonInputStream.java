package org.suche.json;

import java.io.ByteArrayInputStream;
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

	final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack = stack64;
		int depth = -1;
		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }
		void push(final long typeDesc, final Object obj, final Object meta, final int targetIdx, final int limit) {
			if (++depth >= stack.length) {
				if(depth > limit) throwInvalid("StackOverflowError");
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

	@Override public int depth() { return engineStack.depth; }

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.engineStack.depth = -1;
		s.lastArraySize = 16;
		s.metaCache = engine.metaCache();
		return s;
	}

	static JsonInputStream of(final byte[] bs, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		final var l = Math.min(bs.length, s.buffer.length);
		final var in = bs.length <= s.buffer.length ? null : new ByteArrayInputStream(bs);
		s.init(in, engine);
		if(in == null) {
			s.limit = l;
			System.arraycopy(bs, 0, s.buffer, 0, l);
		}
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

	private boolean consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == ',') { pos++; return true; }
		return consumeCommaSlow();
	}

	private boolean consumeCommaSlow() throws IOException {
		var p = pos;
		final var l = limit;
		final var buf = buffer;
		while (p < l) {
			final var c = buf[p];
			if (c == ',') { pos = p + 1; return true; }
			if ((c & 0xC0) != 0 || ((1L << c) & WHITESPACE_MASK) == 0) { pos = p; return false; }
			p++;
		}
		pos = p;
		skipWhitespace();
		if (pos < limit && buffer[pos] == ',') { pos++; return true; }
		return false;
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

	// ############################### Non Recursive state engine ##############################

	private Object runStateEngine(final long startTypeDesc, final Object startObj, final Object startMeta, final int startIdx) throws Throwable {
		final var stackLimit = engine.config().maxDepth();
		var curTypeDesc = startTypeDesc;
		var curObj = startObj;
		var curMeta = (ObjectMeta) startMeta;
		var curIdx = startIdx;
		engineStack.clear();
		var needsComma = false;
		var trailingComma = false;

		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid("Unexpected end.5"); }
			final var b = buffer[pos];
			switch (b) {
			case '\t', '\n', '\r', ' ' -> {
				pos++;
				skipWhitespace();
			}
			case '"' -> {
				if (needsComma) throwInvalid("Expected comma bevore STRING");
				if (curTypeDesc >= 0L && curIdx < 0) {
					curIdx = parseStringKeyAsIndex(curObj, curMeta);
					expect((byte) ':');
					// We successfully parsed the key and colon, next up is the value.
					needsComma = false;
					trailingComma = false;
				} else {
					curMeta.set(this, curObj, curIdx, parseStringValue());
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					final var hasComma = consumeCommaIfPresent();
					needsComma = !hasComma;
					trailingComma = hasComma;
				}
			}
			case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
				if (needsComma) throwInvalid("Expected comma before NUMBER");
				if (curTypeDesc >= 0L && curIdx < 0) throwInvalid( "Expected key before NUMBER");
				parseNumericPrimitive(curMeta, curObj, curIdx, curTypeDesc);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				final var hasComma = consumeCommaIfPresent();
				needsComma = !hasComma;
				trailingComma = hasComma;
			}
			case 'n' -> {
				if (needsComma) throwInvalid("Expected comma before NULL");
				if (curTypeDesc >= 0L && curIdx < 0) throwInvalid("Expected key before NULL");
				fillNullValue(curTypeDesc, curObj, curIdx, curMeta);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				final var hasComma = consumeCommaIfPresent();
				needsComma = !hasComma;
				trailingComma = hasComma;
			}
			case 't', 'f' -> {
				if (needsComma) throwInvalid("Expected comma before BOOLEAN");
				if (curTypeDesc >= 0L && curIdx < 0) throwInvalid("Expected key before BOOLEAN");
				curMeta.set(this, curObj, curIdx, parseTrueOrFalse(b == 't'));
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				final var hasComma = consumeCommaIfPresent();
				needsComma = !hasComma;
				trailingComma = hasComma;
			}
			case '{' -> {
				if (needsComma) throwInvalid("Expected comma before OBJECT");
				if (curTypeDesc >= 0L && curIdx < 0) throwInvalid("Expected key before OBJECT");
				engineStack.push(curTypeDesc, curObj, curMeta, curIdx, stackLimit);
				curTypeDesc = curMeta.fieldDescriptor(curIdx);
				pos++;
				if (curTypeDesc < 0L || (curTypeDesc & 1L) != 0L) throwInvalid("Expected got unexpected '{'");
				curMeta = metaCache[(int) (curTypeDesc >> 1)];
				curObj = curMeta.start(this);
				curIdx = -1;
				needsComma = false;
				trailingComma = false;
			}
			case '}', ']' -> {
				if (trailingComma) throwInvalid("Trailing comma");
				if (b == '}') {
					if (curTypeDesc    <  0L) throwInvalid("Expected ']', got '}'");
					if (curIdx         >= 0 ) throwInvalid("Expected value, got '}'");
				} else if (curTypeDesc >= 0L) throwInvalid("Expected '}', got ']'");

				pos++;
				final var finishedObj = curMeta.end(this, curObj);
				if (engineStack.depth < 0) return finishedObj;
				final var parent = engineStack.stack[engineStack.depth--];
				curTypeDesc = parent.typeDesc;
				curObj      = parent.obj;
				curMeta     = (ObjectMeta) parent.meta;
				curIdx      = parent.targetIdx;
				curMeta.set(this, curObj, curIdx, finishedObj);
				curIdx      = curTypeDesc < 0L ? curIdx + 1 : -1;
				// Act as if we just finished parsing a value in the parent context
				final var hasComma = consumeCommaIfPresent();
				needsComma = !hasComma;
				trailingComma = hasComma;
			}
			case '[' -> {
				if (needsComma) throwInvalid("Expected comma");
				if (curTypeDesc >= 0L && curIdx < 0) throwInvalid("Expected key");
				pos++;
				var childDesc = curMeta.fieldDescriptor(curIdx);
				if (childDesc >= 0L) {
					if (((int) (childDesc >> 1))!= ObjectMeta.IDX_MAP) throwInvalid(OBJECT_INSTEAD_OF_ARRAY);
					childDesc = ObjectMeta.DESC_COLLECTION;
				}
				if ((childDesc & 1L) != 0L) {
					curMeta.set(this, curObj, curIdx, parsePrimitiveArray(childDesc));
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					final var hasComma = consumeCommaIfPresent();
					needsComma = !hasComma;
					trailingComma = hasComma;
				} else {
					engineStack.push(curTypeDesc, curObj, curMeta, curIdx, stackLimit);
					curTypeDesc   = childDesc;
					curMeta       = metaCache[(int) (curTypeDesc >> 1)];
					curObj        = curMeta.start(this);
					curIdx        = 0;
					needsComma    = false;
					trailingComma = false;
				}
			}
			default -> throwInvalid("Unexpected CHAR.1");
			}
		}
	}

	// ############################### Public Part #############################################

	@SuppressWarnings("unchecked")
	public <T> T readObject(final Class<T> targetType) throws Throwable {
		skipWhitespace();
		if (pos >= limit) throwInvalid("Empty JSON document");

		final var b = buffer[pos];
		var targetMeta = engine.metaOf(targetType);

		if (b == '[' && targetMeta != null && (targetMeta.cacheIndex == ObjectMeta.IDX_MAP || targetMeta.cacheIndex == ObjectMeta.IDX_GENERIC)) {
			targetMeta = metaCache[ObjectMeta.IDX_COLLECTION];
		}
		final var isArray = (b == '[');
		final var startTypeDesc = EngineImpl.createTypeDesc(isArray, false, targetMeta != null ? targetMeta.cacheIndex : 0);

		final T result;

		// By-pass the state engine entirely for root-level primitive values
		if (b != '{' && b != '[') result = (T) parsePrimitiveInline(b, startTypeDesc);
		else if (this.maxRecursiveDepth <= 0) {
			pos++;
			result = (T) runStateEngine(startTypeDesc, targetMeta != null ? targetMeta.start(this) : null, targetMeta, isArray ? 0 : -1);
		} else if (b == '{') {
			pos++;
			result = (T) parseRecordRecursive(targetMeta != null ? targetMeta : metaCache[ObjectMeta.IDX_MAP], 0);
		} else { // b == '['
			pos++;
			result = (T) parseArrayRecursive(targetMeta != null ? targetMeta : metaCache[ObjectMeta.IDX_COLLECTION], 0);
		}

		if (!engine.ignoreTrailing()) checkTrailing();
		return result;
	}

	private  void checkTrailing() throws IOException {
		skipWhitespace();
		if (pos < limit) throwInvalid("Trailing garbage after JSON document");
	}

	private Object parsePrimitiveInline(final byte b, final long typeDesc) throws IOException {
		return switch (b) {
		case '"' -> parseStringValue();
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(typeDesc);
		};
	}

	@SuppressWarnings("unchecked")
	public <T> T[] readRecords(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var metaIdx = ((EngineImpl) engine).getDynamicMetaId(targetClass, ObjectMeta.TYPE_OBJ_ARRAY);
		final var meta = metaCache[metaIdx];
		final var typeDesc = EngineImpl.createTypeDesc(true, false, metaIdx);
		try {
			final var ret = (T[]) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
			if(!engine.ignoreTrailing()) checkTrailing();
			return ret;
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
			final var ret = (Map<String, V>) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, -1)
					: parseRecordRecursive(meta, 0));
			if(!engine.ignoreTrailing()) checkTrailing();
			return ret;
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
			final var ret = (List<T>) (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
			if(!engine.ignoreTrailing()) checkTrailing();
			return ret;
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
			final var ret =  (Set<T>)  (this.maxRecursiveDepth <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(meta, 0));
			if(!engine.ignoreTrailing()) checkTrailing();
			return ret;
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	// ========================================================================
	// RECURSIVE FAST-PATH ENGINE (Mit Hybrid-Fallback zur State-Machine)
	// ========================================================================
	private static final String OBJECT_INSTEAD_OF_ARRAY = "Expected object, got '['";

	private Object parseRecordRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		final var context = meta.start(this);
		final var limitDepth = this.maxDepth;
		var expectKey = false;
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid("Unexpedted END.2"); }
			int targetIdx;
			var b = buffer[pos];
			if ((b & 0xC0) == 0 && ((1L << b) & WHITESPACE_MASK) != 0) skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) '}') {
				if (expectKey) throwInvalid("Trailing comma in object");
				pos++;
				return meta.end(this, context);
			}
			if (buffer[pos] != '"') throwInvalid("Expected '\"' for object key but got: " + (char) buffer[pos]);
			targetIdx = parseStringKeyAsIndex(context, meta);
			ensure(3); // Minimum :1}

			b = buffer[pos++];
			if ((b & 0xC0) == 0 && ((1L << b) & WHITESPACE_MASK) != 0) {
				skipWhitespace();
				ensure(3); // Minimum :1}
				b = buffer[pos++];
			}

			if (b != ':') unexpect(b, (byte)':');

			// expect((byte) ':');
			if (targetIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); continue; }
			if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid("Unexpedted END.1"); }
			b = buffer[pos];
			if ((b & 0xC0) == 0 && ((1L << b) & WHITESPACE_MASK) != 0) {
				skipWhitespace();
				b = buffer[pos];
			}
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, targetIdx, meta.fieldDescriptor(targetIdx));
			case 'n' -> fillNullValue(meta.fieldDescriptor(targetIdx), context, targetIdx, meta);
			case 't' -> meta.set(this, context, targetIdx, parseTrue());
			case 'f' -> meta.set(this, context, targetIdx, parseFalse());
			case '"' -> meta.set(this, context, targetIdx, parseStringValue());
			case '{' -> {
				final var fieldDesc = meta.fieldDescriptor(targetIdx);
				if (fieldDesc < 0L || (fieldDesc & 1L) != 0L) throwInvalid("Expected array, got '{'");
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
				if (fieldDesc >= 0L && metaIdx != ObjectMeta.IDX_GENERIC && metaIdx != ObjectMeta.IDX_MAP) throwInvalid(OBJECT_INSTEAD_OF_ARRAY);
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
			default -> throwInvalid("Unexpected CHAR");
			}
			if (pos < limit && buffer[pos] == ',') {
				pos++;
				expectKey = true;
			} else {
				expectKey = consumeCommaSlow();
			}
		}
	}

	private Object parseArrayRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		depth = recursionDeep;
		final var context    = meta.start(this);
		final var limitDepth = this.maxDepth;
		final var childDesc  = meta.fieldDescriptor(0);
		var idx = 0;
		byte state = 0;
		// LAZY EVALUATION VARIABLES (Calculated only when strictly necessary)
		var objPassDesc = 0L;
		var arrPassDesc = 0L;
		ObjectMeta objFallbackMeta = null;
		ObjectMeta arrFallbackMeta = null;
		var isPrimitive = false;
		var metaIdx = 0;
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throwInvalid("Unexpected end"); }
			switch (buffer[pos]) {
			case '\t','\n','\r',' ' -> { pos++; }							// SKIP   Whitespace
			case ']' -> {
				if (state == 0 && idx > 0) throwInvalid("Trailing comma in array");
				pos++; return meta.end(this, context);
			}	// ALWAYS Possible
			case '-','0','1','2','3','4','5','6','7','8','9' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of NUMERIC expected");
				state = 1;
				parseNumericPrimitive(meta, context, idx, childDesc);
			}
			case ',' -> {
				idx++;
				if(state==0) throwInvalid("VALUE instead of KOMMA expected");
				state = 0;
				pos++;
			}
			case 'n' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of NULL expected");
				state = 1;
				fillNullValue(childDesc, context, idx, meta);
			}
			case 't' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of BOOLEAN expected");
				state = 1;
				meta.set(this, context, idx, parseTrue());
			}
			case 'f' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of BOOLEAN expected");
				state = 1;
				meta.set(this, context, idx, parseFalse());
			}
			case '"' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of STRING expected");
				state = 1;
				meta.set(this, context, idx, parseStringValue());
			}
			case '{' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of OBJECT expected");
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

				if (childDesc < 0L || isPrimitive) throwInvalid("Expected array, got '{'");
				if (recursionDeep >= limitDepth) {
					meta.set(this, context, idx, runStateEngine(objPassDesc, objFallbackMeta.start(this), objFallbackMeta, -1));
				} else {
					pos++;
					meta.set(this, context, idx, parseRecordRecursive(objFallbackMeta, recursionDeep + 1));
				}
			}
			case '[' -> {
				if(state==1) throwInvalid("Komma or Closeing breaket instead of ARRAY expected");
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

				if ((childDesc >= 0L) && metaIdx != ObjectMeta.IDX_GENERIC && metaIdx != ObjectMeta.IDX_MAP) throwInvalid(OBJECT_INSTEAD_OF_ARRAY);
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
			default -> throwInvalid("Unexpected byte["+buffer[pos]+"] instead of "+(state==0?"VALUE":"KOMMA/END"));
			}
		}
	}

}