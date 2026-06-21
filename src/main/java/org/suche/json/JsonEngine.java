package org.suche.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.suche.json.JsonOutputStream.Flags;
import org.suche.json.JsonOutputStream.TimeFormat;

public sealed interface JsonEngine permits InternalEngine {
	JsonEngine DEFAULT = JsonEngine.of(MetaConfig.DEFAULT);
	JsonInputStream jsonInputStream(InputStream is);
	JsonInputStream jsonInputStream(byte[]      bytes);
	static JsonEngine of (final MetaConfig cfg) { return new EngineImpl(cfg); }

	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags);
	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags);
	JsonOutputStream jsonOutputStream(final OutputStream out);

	void maxRecursiveDepth(int v);
	int maxRecursiveDepth();

	<C> void registerTransformer(final Class<C> c, final UnaryOperator<Object> f);

	void skipInvalid(final boolean v);
	boolean skipInvalid();

	void ignoreTrailing(final boolean v);
	boolean ignoreTrailing();


	void failOnUnknownProperties(final boolean v);
	boolean failOnUnknownProperties();

	void autoPojo(final Predicate<Class<?>> p);

	static class JSONException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		final Throwable cause;
		JSONException(final Throwable t) {
			super(t.getMessage(), null, false, false);
			cause = t;
			setStackTrace(t.getStackTrace());
		}

		JSONException(final String t) { super(t); cause = null; }
	}

	static JSONException illegalStateException(final Throwable t) { throw (t instanceof final RuntimeException e  ? e : new JSONException(t)); }
	static JSONException illegalStateException(final String    t) { throw new JSONException(t); }

	@SuppressWarnings("unchecked")
	static <T> T of(final InputStream src, final Class<?> t) {
		try(	var s = DEFAULT.jsonInputStream(src)) { return (T)s.readObject(t); } catch(final Throwable x) { illegalStateException(x); return null; }
	}

	@SuppressWarnings("unchecked")
	static <T> T of(final byte[] src, final Class<?> t) {
		try(	var s = DEFAULT.jsonInputStream(src)) { return (T)s.readObject(t); } catch(final Throwable x) { illegalStateException(x); return null; }
	}

	static <T> T of(final String src, final Class<?> t) { return of(src.getBytes(StandardCharsets.UTF_8), t); }

	static <T> T of(final Path src, final Class<?> t) {
		try(	final var i = Files.newInputStream(src)) { return of(i, t); } catch(final Exception x) { illegalStateException(x); return null; }
	}

	static JSONArray  ofJSONArray     (final Path   json) { return of(json           , JSONArray.class); }
	static JSONArray  ofJSONArray     (final String json) { return of(json.getBytes(), JSONArray.class); }
	static JSONArray  ofJSONArray     (final byte[] json) { return of(json           , JSONArray.class); }

	static JSONObject ofJSONObject    (final Path   json) { return of(json           , JSONObject.class); }
	static JSONObject ofJSONObject    (final String json) { return of(json.getBytes(), JSONObject.class); }
	static JSONObject ofJSONObject    (final byte[] json) { return of(json           , JSONObject.class); }

	static JSONArray  pathToJSONArray (final Path   json) { return of(json           , JSONArray.class); }
	static JSONArray  textToJSONArray (final String json) { return of(json.getBytes(), JSONArray.class); }
	static JSONArray  byteToJSONArray (final byte[] json) { return of(json           , JSONArray.class); }

	static JSONObject pathToJSONObject(final Path   json) { return of(json           , JSONObject.class); }
	static JSONObject textToJSONObject(final String json) { return of(json.getBytes(), JSONObject.class); }
	static JSONObject byteToJSONObject(final byte[] json) { return of(json           , JSONObject.class); }

	static byte[] toBytes(final Object l) {
		final var o = new ByteArrayOutputStream();
		try(var s = DEFAULT.jsonOutputStream(o)) { s.writeObject(l); } catch (final IOException e) { illegalStateException(e); }
		return o.toByteArray();
	}

	static String toString(final Object l) {
		final var b = toBytes(l);
		return new String(b,0,b.length);
	}

	static JSONObject newJSONObject() { return new CompactMap(PRIMITIVE.T_EMPTY, null, null); }
}