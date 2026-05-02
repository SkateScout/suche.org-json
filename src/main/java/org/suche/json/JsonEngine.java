package org.suche.json;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.suche.json.JsonOutputStream.Flags;
import org.suche.json.JsonOutputStream.TimeFormat;

public sealed interface JsonEngine permits InternalEngine {
	JsonInputStream jsonInputStream(InputStream is);
	static JsonEngine of (final MetaConfig cfg) { return new EngineImpl(cfg); }

	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags);
	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags);
	JsonOutputStream jsonOutputStream(final OutputStream out);

	void maxRecursiveDepth(int v);
	int maxRecursiveDepth();

	<C> void registerTransformer(final Class<C> c, final UnaryOperator<Object> f);

	void skipInvalid(final boolean v);
	boolean skipInvalid();

	void failOnUnknownProperties(final boolean v);
	boolean failOnUnknownProperties();

	void autoPojo(final Predicate<Class<?>> p);
}

sealed interface InternalEngine extends JsonEngine permits EngineImpl {
	ObjectMeta       metaOf   (Class<?> clazz);
	MetaConfig       config   ();
	KeyValueObject[] ofComplex( final Class<?> c);
	UnaryOperator<Object> transformer(Class<?> c);
	boolean                 hasCoreTransformer();
	ObjectMeta[] metaCache();
}
