package org.suche.json;

final class PRIMITIVE extends Number {
	public static final PRIMITIVE LONG    = new PRIMITIVE();
	public static final PRIMITIVE DOUBLE  = new PRIMITIVE();

	private static final long serialVersionUID = 1L;
	private PRIMITIVE() { }
	@Override public int    intValue   () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public long   longValue  () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public float  floatValue () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public double doubleValue() { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
}