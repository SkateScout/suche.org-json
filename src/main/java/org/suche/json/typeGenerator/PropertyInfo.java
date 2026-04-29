package org.suche.json.typeGenerator;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class PropertyInfo {
	static final int T_BYTE    = 1 <<  0;
	static final int T_SHORT   = 1 <<  1;
	static final int T_INT     = 1 <<  2;
	static final int T_LONG552 = 1 <<  3;
	static final int T_LONG    = 1 <<  4;
	static final int T_FLOAT   = 1 <<  5;
	static final int T_DOUBLE  = 1 <<  6;
	static final int T_CHAR    = 1 <<  7;
	static final int T_BOOLEAN = 1 <<  8;
	static final int T_STRING  = 1 <<  9;
	static final int T_ENUM    = 1 << 10;
	static final int T_ARRAY   = 1 << 11;
	static final int T_OBJECT  = 1 << 12;
	static final int T_NULL    = 1 << 13;
	static final int T_MAP     = 1 << 14;
	static final String CLASS_KEY  = "__class__";
	static final String ENUM_KEY   = "__enum__";
	static final String VALUE_KEY  = "value";
	static final int    NUMERICS_MASK   = (PropertyInfo.T_BYTE | PropertyInfo.T_SHORT | PropertyInfo.T_INT | PropertyInfo.T_LONG | PropertyInfo.T_FLOAT | PropertyInfo.T_DOUBLE);
	public final String fieldName;
	int         types      = 0;
	boolean     hasDefault = false;
	int         useCount;
	int         minSize    = Integer.MAX_VALUE;
	int         maxSize    = Integer.MIN_VALUE;
	TreeSet<String> classes;
	Set<String>     enums;
	Set<String>     enumClasses;
	PropertyInfo    arrayElements = null;
	PropertyInfo    mapValues     = null;
	String          interfaceName = null;
	Set<String>     autoStringValues = new HashSet<>();
	boolean         autoStringAborted = false;

	PropertyInfo(final String name) { this.fieldName = name; }

	private boolean registerString(final String v) {
		types |= T_STRING;
		if(v.isEmpty()) hasDefault = true;
		if(!autoStringAborted) {
			if(!PrettyNaming.isValidVariableName(v)) {
				autoStringAborted = true;
				autoStringValues = null;
			} else {
				autoStringValues.add(v);
				if(autoStringValues.size() >= 33) {
					autoStringAborted = true;
					autoStringValues = null;
				}
			}
		}
		return false;
	}

	private boolean registerMap(final String fName, final Map<String, Set<String>> globalEnums, final Map<?,?> m) {
		@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
		minSize = Math.min(minSize, t.size());
		maxSize = Math.max(maxSize, t.size());
		if(t.get(CLASS_KEY) instanceof final String cls) {
			if(classes == null) classes = new TreeSet<>();
			classes.add(cls);
			types |= T_OBJECT;
			return true;
		}
		if(t.get(ENUM_KEY) instanceof final String cls) {
			if(enumClasses == null) enumClasses = new HashSet<>();
			enumClasses.add(cls);
			types |= T_ENUM;
			if(t.get(VALUE_KEY) instanceof final String enumValue) {
				(enums == null ? enums = new HashSet<>() : enums).add(enumValue);
				globalEnums.computeIfAbsent(cls, _->new TreeSet<>()).add(enumValue);
			}
			return false;
		}
		types |= T_MAP;
		if(mapValues == null) mapValues = new PropertyInfo(fName);
		for(final var val : t.values()) mapValues.register(fName, globalEnums, val);
		return true;
	}

	private boolean registerCollection(final String fName, final Map<String, Set<String>> globalEnums, final Collection<?> t) {
		if(t.isEmpty()) return false;
		types |= T_ARRAY;
		minSize = Math.min(minSize, t.size());
		maxSize = Math.max(maxSize, t.size());
		if(arrayElements == null) arrayElements = new PropertyInfo("[]");
		for(final var e : t) arrayElements.register(fName, globalEnums, e);
		return true;
	}

	boolean register(final String fName, final Map<String, Set<String>> globalEnums, final Object v) {
		useCount++;
		return switch(v) {
		case null -> { types |= T_NULL; yield false; }
		case final Number t -> {
			final var d = t.doubleValue();
			final var l = t.longValue();
			if(0 == d) hasDefault = true;
			if(((long)d) != l) { types |= T_DOUBLE; yield false; }
			if(l >= Byte   .MIN_VALUE && l <= Byte   .MAX_VALUE) { types |= T_BYTE ; yield false; }
			if(l >= Short  .MIN_VALUE && l <= Short  .MAX_VALUE) { types |= T_SHORT; yield false; }
			if(l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) { types |= T_INT  ; yield false; }
			types |= T_LONG;
			yield false;
		}
		case final Boolean t -> {
			types |= T_BOOLEAN;
			if(!t) hasDefault = true;
			yield false;
		}
		case final String t -> registerString(t);
		case final Collection<?> t -> registerCollection(fName, globalEnums, t);
		case final Map<?,?> m -> registerMap(fName, globalEnums, m);
		default -> false;
		};
	}

	String resolveJavaType(final int maxUseCount, final boolean forceObject) {
		final var mayAbsent = useCount < maxUseCount;
		final var nullable  = forceObject || (types & T_NULL) != 0 || (this.hasDefault && mayAbsent);
		var exactType = this.types & ~T_NULL;
		final var numerics = exactType & NUMERICS_MASK;
		if (numerics != 0 && numerics == exactType) exactType = Integer.highestOneBit(exactType);
		return switch (exactType) {
		case T_DOUBLE  -> nullable ? "Double"  : "double";
		case T_LONG    -> nullable ? "Long"    : "long";
		case T_INT     -> nullable ? "Integer" : "int";
		case T_BOOLEAN -> nullable ? "Boolean" : "boolean";
		case T_STRING  -> "String";
		case T_ENUM    -> interfaceName != null ? interfaceName : (enumClasses != null ? enumClasses.iterator().next() : "String");
		case T_MAP     -> "Map<String, " + (mapValues != null ? mapValues.resolveJavaType(maxUseCount, true) : "Object") + ">";
		case T_ARRAY   -> (arrayElements == null ? "Object[]" : arrayElements.resolveJavaType(maxUseCount, forceObject) + "[]");
		case T_OBJECT  -> (classes != null && classes.size() == 1) ? classes.iterator().next() : (interfaceName != null ? interfaceName : "Object");
		default        -> "Object";
		};
	}

	String getReferencedClass() {
		final var exactType = this.types & ~T_NULL;
		final var numerics = exactType & NUMERICS_MASK;
		if (numerics != 0 && numerics == exactType) return null;
		return switch (exactType) {
		case T_ENUM    -> interfaceName != null ? interfaceName : (enumClasses == null ? null : enumClasses.iterator().next());
		case T_MAP     -> (mapValues     == null ? null : mapValues    .getReferencedClass());
		case T_ARRAY   -> (arrayElements == null ? null : arrayElements.getReferencedClass());
		case T_OBJECT  -> (classes != null && classes.size() == 1) ? classes.iterator().next() : interfaceName;
		case T_BOOLEAN,T_STRING -> null;
		default                 -> null;
		};
	}
}