package org.suche.json;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.function.BiFunction;

public record MetaConfig(boolean emitClassName,
		boolean                  skipDefaultValues ,
		/**
		 * @return String     -> nameName
		 *         NameFilter -> nameName,filter
		 *         Filter     ->          filter
		 */
		BiFunction<String, RecordComponent, Object> mapComponent,
		/**
		 * @return String     -> nameName
		 *         NameFilter -> nameName,filter
		 *         Filter     ->          filter
		 */
		BiFunction<String, Method, Object> mapMethod     ,
		/**
		 * @return String     -> nameName
		 *         NameFilter -> nameName,filter
		 *         Filter     ->          filter
		 */
		BiFunction<String, Field , Object> mapField      ,
		int                                maxDepth         ,
		int                                maxStringLength  ,
		int                                maxCollectionSize,
		boolean                            enableDeduplication) {

	public interface Filter { Object apply(Object v); }
	public record NameFilter(String name, Filter filter) { }

	public static final RuntimeException SKIP_FIELD = new RuntimeException("ILLEGALSTATE", null, false, false) { };

	public static final MetaConfig DEFAULT = new MetaConfig(false, false, null, null, null, 128, 5 * 1024 * 1024, 100_000, true);
}