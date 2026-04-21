package org.suche.json;

import org.suche.json.ObjectMeta.ArrayBuilder;

interface MetaPool {
	record CtxArrays(Object[] objects, long[] primitives) {}

	Object[]     takeArray(int minCapacity);
	ArrayBuilder takeBuilder();
	CtxArrays    takeCtxArrays(int size);

	void         returnArray(Object[] arr);
	void         returnBuilder(ArrayBuilder b);
	void         returnCtxArrays(CtxArrays c);

	Object deduplicate(final Object value);

	String internBytes(byte[] src, int start, int len, int hash, boolean isAscii);
	InternalEngine engine();
}