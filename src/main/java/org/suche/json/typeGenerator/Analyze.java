package org.suche.json.typeGenerator;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

public class Analyze {
	private static final int    MIN_PREFIX_LEN  = 2;
	private static final int    MIN_POSTIFX_LEN = 3;
	private static final String RECORD_PREFIX   = "record ";
	private static final String ENUM_PREFIX     = "enum ";
	private static final int    NUMERICS_MASK   = (PropertyInfo.T_BYTE | PropertyInfo.T_SHORT | PropertyInfo.T_INT | PropertyInfo.T_LONG | PropertyInfo.T_FLOAT | PropertyInfo.T_DOUBLE);

	public static final class PropertyInfo {
		static final int T_BYTE    = 1 <<  0; // Byte .MIN_VALUE   <= v <= Byte.MAX_VALUE
		static final int T_SHORT   = 1 <<  1; // Short.MIN_VALUE   <= v <= Short.MAX_VALUE
		static final int T_INT     = 1 <<  2; // Integer.MIN_VALUE <= v <= Integer.MAX_VALUE
		static final int T_LONG552 = 1 <<  3; // long may double without loose
		static final int T_LONG    = 1 <<  4; // Long.MIN_VALUE <= v <= Byte.MAX_VALUE
		static final int T_FLOAT   = 1 <<  5; // Unused
		static final int T_DOUBLE  = 1 <<  6;
		static final int T_CHAR    = 1 <<  7;
		static final int T_BOOLEAN = 1 <<  8;
		static final int T_STRING  = 1 <<  9;
		static final int T_ENUM    = 1 << 10;
		static final int T_ARRAY   = 1 << 11; // No difference between []/CollectionS
		static final int T_OBJECT  = 1 << 13;
		static final int T_NULL    = 1 << 14; // Explizit null used
		static final int T_MAP     = 1 << 15;
		static final String CLASS_KEY  = "__class__";
		static final String ENUM_KEY   = "__enum__";
		static final String VALUE_KEY  = "value";

		private static final long LONG552_MAX_VALUE = 1<<62;
		private static final long LONG552_MIN_VALUE = -LONG552_MAX_VALUE;

		public final String fieldName;
		public int         types      = 0;
		public boolean     hasDefault = false; // 0 or false
		public int         useCount;   // Statistik only
		public int         minSize    = Integer.MAX_VALUE;
		public int         maxSize    = Integer.MIN_VALUE;

		public TreeSet<String> classes    ;	// If not Primitive "__class__" names seen
		public Set<String>     enums      ;	// Enumeration Values seen
		public Set<String>     enumClasses;	// Enumeration Values seen
		public PropertyInfo    arrayElements = null;
		public PropertyInfo    mapValues     = null;
		public String          interfaceName = null;

		PropertyInfo(final String name) { this.fieldName = name; }

		// Return if to dive info
		boolean register(final String fieldName, final Map<String, Set<String>> globalEnums, final Object v) {
			useCount++;
			return switch(v) {
			case null -> { types |= T_NULL; yield false; }
			case final Number        t -> {
				final var d = t.doubleValue();
				final var l = t.longValue  ();
				if(0 == d) hasDefault = true;
				if(((long)d) != l                                 ) { types |= T_DOUBLE ; yield false; }
				if(Byte   .MIN_VALUE <= l&& l <= Byte   .MAX_VALUE) { types |= T_BYTE   ; yield false; }
				if(Short  .MIN_VALUE <= l&& l <= Short  .MAX_VALUE) { types |= T_SHORT  ; yield false; }
				if(Integer.MIN_VALUE <= l&& l <= Integer.MAX_VALUE) { types |= T_INT    ; yield false; }
				if(LONG552_MIN_VALUE <= l&& l <= LONG552_MAX_VALUE) { types |= T_LONG552; yield false; }
				types |= T_LONG;
				yield false;
			}
			case final Boolean       t -> { types |= T_BOOLEAN; if(!t.booleanValue()) hasDefault = true;  yield false; }
			case final String        t -> { types |= T_STRING ; if(t.isEmpty      ()) hasDefault = true;  yield false; }
			case final Character     _ -> { types |= T_CHAR   ; yield false; }
			case final Enum<?>       t -> { types |= T_ENUM   ; if(enums == null) enums = new HashSet<>(); enums.add(t.name()); yield false;}
			case final Collection<?> t -> {
				types |= T_ARRAY;
				minSize = Math.min(minSize, t.size());
				maxSize = Math.max(maxSize, t.size());
				if(t.isEmpty()) hasDefault = true;
				if(arrayElements == null) arrayElements = new PropertyInfo("[]");
				for(final var e : t) arrayElements.register(fieldName, globalEnums, e);
				yield true;
			}
			case final Map<?,?> m -> {
				@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
				minSize = Math.min(minSize, t.size());
				maxSize = Math.max(maxSize, t.size());

				if(t.get(CLASS_KEY) instanceof final String cls) {
					if(classes == null) classes = new TreeSet<>();
					classes.add(cls);
					types |= T_OBJECT;
					yield true;
				}
				if(t.get(ENUM_KEY ) instanceof final String cls) {
					if(enumClasses == null) enumClasses = new HashSet<>();
					enumClasses.add(cls);
					types |= T_ENUM;
					if(t.get(VALUE_KEY) instanceof final String enumValue) {
						(enums == null ? enums = new HashSet<>() : enums).add(enumValue);
						globalEnums.computeIfAbsent(cls, _->new TreeSet<>()).add(enumValue);
					}
					yield false;
				}
				types |= T_MAP;
				if(mapValues == null) mapValues = new PropertyInfo(fieldName);
				for(final var val : t.values()) mapValues.register(fieldName, globalEnums, val);
				yield true;
			}
			case final Object[] arr -> {
				types |= T_ARRAY;
				if(arrayElements == null) arrayElements = new PropertyInfo("[]");
				for(final var e : arr) arrayElements.register(fieldName, globalEnums, e);
				yield true;
			}

			default -> {
				if(v.getClass().isArray()) { types |= T_ARRAY; yield true; }
				System.err.println("TYP- ? "+v.getClass().getCanonicalName()+" "+v);
				yield false;
			}
			};
		}

		// this.useCount < useCount := This implies optiona nullable
		public String resolveJavaType(final int maxUseCount) {
			final var t = this.types;
			final var mayAbsent = useCount < maxUseCount;
			final var nullable = (t & PropertyInfo.T_NULL) != 0 || (this.hasDefault && mayAbsent);	// Wenn wir explizit 'null' gesehen haben oder das Feld in einigen Objekten fehlte

			final var numerics = t & NUMERICS_MASK;
			if (numerics != 0 && numerics == t) {
				// Das höchste gesetzte Bit gewinnt! (z.B. wenn BYTE und INT gesetzt sind, liefert das INT)
				final var highest = Integer.highestOneBit(numerics);
				switch (highest) {
				case PropertyInfo.T_DOUBLE: return nullable ? "Double"  : "double";
				case PropertyInfo.T_FLOAT : return nullable ? "Float"   : "float";
				case PropertyInfo.T_LONG  : return nullable ? "Long"    : "long";
				case PropertyInfo.T_INT   : return nullable ? "Integer" : "int";
				case PropertyInfo.T_SHORT : return nullable ? "Short"   : "short";
				case PropertyInfo.T_BYTE  : return nullable ? "Byte"    : "byte";
				default                   : break;
				}
			}
			if ((t & PropertyInfo.T_BOOLEAN) == PropertyInfo.T_BOOLEAN) return nullable ? "Boolean"   : "boolean";
			if ((t & PropertyInfo.T_STRING ) == PropertyInfo.T_STRING ) return "String";
			if ((t & PropertyInfo.T_CHAR   ) == PropertyInfo.T_CHAR   ) return nullable ? "Character" : "char";
			if ((t & PropertyInfo.T_ENUM   ) == PropertyInfo.T_ENUM   ) return this.enumClasses != null ? this.enumClasses.iterator().next() : "String";
			if ((t & PropertyInfo.T_MAP    ) == PropertyInfo.T_MAP    ) {
				final var valType = (mapValues != null && mapValues.types != 0) ? mapValues.resolveJavaType(maxUseCount) : "Object";
				return "Map<String, " + valType + ">";
			}
			if ((t & PropertyInfo.T_ARRAY  ) == PropertyInfo.T_ARRAY) {
				final var elemType = (arrayElements != null && arrayElements.types != 0) ? arrayElements.resolveJavaType(maxUseCount) : "Object";
				return switch(elemType) {
				case "int"     -> "int[]";
				case "long"    -> "long[]";
				case "double"  -> "double[]";
				case "float"   -> "float[]";
				case "boolean" -> "boolean[]";
				case "byte"    -> "byte[]";
				case "short"   -> "short[]";
				case "char"    -> "character[]";
				default        -> elemType+"[]";
				};
			}
			if ((t & PropertyInfo.T_OBJECT ) == PropertyInfo.T_OBJECT ) {
				if (this.classes.size() == 1) return this.classes.iterator().next();
				if(interfaceName == null) interfaceName = findSealedInterfaceName(classes, fieldName);
				return interfaceName;
			}
			return "Object_"+t; // Fallback, falls Typ unklar ist (z.B. leere Liste)
		}

		public boolean hasConflict() {
			final var t = this.types;
			final var numerics = t & NUMERICS_MASK;
			if (    (numerics != 0 && numerics == t) ||
					(t & PropertyInfo.T_BOOLEAN) == PropertyInfo.T_BOOLEAN ||
					(t & PropertyInfo.T_STRING ) == PropertyInfo.T_STRING  ||
					(t & PropertyInfo.T_CHAR   ) == PropertyInfo.T_CHAR
					) return false;
			if (    (t & PropertyInfo.T_OBJECT ) == PropertyInfo.T_OBJECT ||
					(t & PropertyInfo.T_MAP    ) == PropertyInfo.T_MAP    ||
					(t & PropertyInfo.T_ARRAY  ) == PropertyInfo.T_ARRAY  ||
					(t & PropertyInfo.T_ENUM   ) == PropertyInfo.T_ENUM   ) return false;
			System.out.println("Conflict t: "+t);
			return true;
		}
	}

	private static final void mapInfo(final Map<String, ObjectInfo> classes,  final Map<String,ObjectInfo> objectInfo, final String parentPath, final Map<?,?> m, final Map<String, Set<String>> enums, final Queue<Map.Entry<String, Object>> todo) {
		@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
		var name = "<MAP>";
		if (t.get(PropertyInfo.CLASS_KEY) instanceof final String s) name = s;
		else if (t.get(PropertyInfo.ENUM_KEY ) instanceof final String s) name = s;
		final var path         = parentPath + "/" + name;
		final var finalName = name;
		final var localObject  = objectInfo.computeIfAbsent(path, _->new ObjectInfo(finalName));
		final var globalObject = classes   .computeIfAbsent(name, _->new ObjectInfo(finalName));
		for(final var prop : t.entrySet()) {
			final var childName = prop.getKey();
			final var childType = prop.getValue();
			if(PropertyInfo.CLASS_KEY.equals         (childName) || PropertyInfo.ENUM_KEY.equals(childName)) continue;
			final var localProp = localObject.properties.computeIfAbsent(childName, PropertyInfo::new);
			if(localProp.register(childName, enums, childType)) todo.add(Map.entry(path + "/" + childName, childType));
			final var globalProp = globalObject.properties.computeIfAbsent(childName, PropertyInfo::new);
			globalProp.register(childName, enums, childType);
			if(!globalObject.hasConflict && globalProp.hasConflict()) globalObject.hasConflict = true;
			if(!localObject .hasConflict && localProp .hasConflict()) localObject .hasConflict = true;
		}
	}

	public static Map<String,ObjectInfo> typeInfo(final Map<String, ObjectInfo> classes, final Map<String, Set<String>> enums, final Object root) {
		final var objectInfo = new HashMap<String,ObjectInfo>();
		final var todo       = new ArrayDeque<Map.Entry<String, Object>>();
		final var localOnce  = Collections.newSetFromMap(new IdentityHashMap<>());
		todo.add(Map.entry("", root));
		while(!todo.isEmpty()) {
			final var stack      = todo.remove();
			final var parentPath = stack.getKey();
			final var value      = stack.getValue();
			if(!localOnce.add(value)) continue;
			switch(value) {
			case final Map<?,?> m -> mapInfo(classes, objectInfo, parentPath, m, enums, todo);
			case final Collection<?> t -> {
				final var path = parentPath + "[]";
				for(final var e : t)
					if (e instanceof Map || e instanceof Collection || (e != null && e.getClass().isArray())) todo.add(Map.entry(path, e));

			}
			default -> {
				if(value.getClass().isArray()) {
					final var l = Array.getLength(value);
					final var path = parentPath + "[]";
					for(var i=0;i<l;i++) todo.add(Map.entry(path, Array.get(value,i)));
				}
			}
			}
		}
		return objectInfo;
	}

	public static String field(final Map.Entry<String, PropertyInfo> pe, final int maxUseCount) {
		return pe.getValue().resolveJavaType(maxUseCount)+" "+pe.getKey();
	}

	private static Set<String> once = new HashSet<>();

	private static void mayImplements(final StringBuilder java, final String name, final Map<String, Set<String>> sealedInterfaces) {
		final var implement = new TreeSet<String>();
		for (final var sealed : sealedInterfaces.entrySet())
			if (sealed.getValue().contains(name))
				implement.add(sealed.getKey());
		if(!implement.isEmpty()) {
			var i = 0;
			for(final var impl : implement) java.append(i++>0?", ":" implements ").append(impl);
		}
	}

	public static int printEnums(final Map<String, Set<String>> enums, final Map<String, Set<String>> sealedInterfaces) {
		final var pre = ENUM_PREFIX.length();
		final var java = new StringBuilder(ENUM_PREFIX);
		for(final var e : enums.entrySet()) {
			final var enumName = e.getKey();
			java.setLength(pre);
			java.append(enumName);
			mayImplements(java, enumName, sealedInterfaces);
			java.append(" {");
			var index = 0;
			final var i = e.getValue().iterator();
			switch(e.getValue().size()) {
			case    1 -> java.append(i.next()).append(" }");
			case    2 -> java.append(i.next()).append(", ").append(i.next()).append(" }");
			default  -> { while(i.hasNext()) java.append((index++>0?", ":" ")).append(i.next());
			}
			}
			System.out.println(java.toString()+" }");
		}
		return enums.size();
	}

	public static String findSealedInterfaceName(final Set<String> classes, final String fieldName) {
		if (classes == null || classes.isEmpty()) return "Object";
		final var iterator = classes.iterator();
		final var first = iterator.next();
		var preLen  = first.length();
		var postLen = first.length();
		while (iterator.hasNext()) {
			final var next    = iterator.next();
			final var nextLen = next.length();
			if (preLen  > MIN_PREFIX_LEN - 1) {	// Calculate common prefix length
				var i = 0;
				final var maxPre = Math.min(preLen, nextLen);
				while (i < maxPre && first.charAt(i) == next.charAt(i)) i++;
				preLen = i;
			}
			if (postLen > MIN_POSTIFX_LEN - 1) {	// Calculate common suffix length
				var i = 0;
				final var maxPost = Math.min(postLen, nextLen);
				while (i < maxPost && first.charAt(first.length() - 1 - i) == next.charAt(nextLen - 1 - i)) i++;
				postLen = i;
			}
			if (preLen  < MIN_PREFIX_LEN && postLen < MIN_POSTIFX_LEN) // No Possible pre/post fix found
				return Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
		}
		final var postOff = first.length() - postLen;
		switch((preLen  >= MIN_PREFIX_LEN ? 2 : 0) + (postLen >= MIN_POSTIFX_LEN ? 1 : 0)) {
		case  3 :
			if (preLen + postLen >= first.length()) return first;
			return      Character.toUpperCase(first.charAt(  0    )) + first.substring(1, preLen)
			+           Character.toUpperCase(first.charAt(postOff)) + first.substring(postOff + 1);
		case  2: return Character.toUpperCase(first.charAt(0      )) + first.substring(1, preLen);
		case  1: return Character.toUpperCase(first.charAt(postOff)) + first.substring(postOff + 1);
		default: return Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
		}
	}

	static void collectSealedInterfaces(final PropertyInfo prop, final Map<String, Set<String>> sealedInterfaces) {
		if (prop == null) return;
		final var allTypes = new TreeSet<String>();
		if (prop.classes     != null) allTypes.addAll(prop.classes);
		if (prop.enumClasses != null) allTypes.addAll(prop.enumClasses);
		if (allTypes.size() > 1) {
			prop.interfaceName = findSealedInterfaceName(allTypes, prop.fieldName);
			sealedInterfaces.computeIfAbsent(prop.interfaceName, _->new TreeSet<>()).addAll(allTypes);
			if(prop.interfaceName == null  ) throw new IllegalStateException("Missing interfaceName");
			if(prop.interfaceName.isBlank()) throw new IllegalStateException("Blank interfaceName");
		}
		collectSealedInterfaces(prop.arrayElements, sealedInterfaces);
		collectSealedInterfaces(prop.mapValues    , sealedInterfaces);
	}

	public static void printInterfaces(final Map<String, ObjectInfo> classes, final Map<String, Set<String>> sealedInterfaces) {
		for (final var entry : sealedInterfaces.entrySet()) {
			final var interfaceName = entry.getKey();
			final var permitsList = entry.getValue();
			final var permits = String.join(", ", permitsList);
			Map<String, String> commonProps = null;
			for (final var className : permitsList) {
				final var oi = classes.get(className);
				if (oi == null || oi.properties == null) {
					// Fallback: Es ist ein Enum oder eine Klasse ohne Eigenschaften -> Keine gemeinsamen Methoden möglich!
					if (commonProps != null) commonProps.clear();
					else commonProps = new TreeMap<>();
					break;
				}
				final var count = oi.getMaxUseCount();
				final var currentProps = new TreeMap<String, String>();
				for (final var propEntry : oi.properties.entrySet()) {
					currentProps.put(propEntry.getKey(), propEntry.getValue().resolveJavaType(count));
				}
				if (commonProps == null) {
					commonProps = currentProps; // Initiale Befüllung mit der ersten Klasse
				} else {
					commonProps.keySet().retainAll(currentProps.keySet()); // Nur behalten, was beide haben
					final var it = commonProps.entrySet().iterator();
					while (it.hasNext()) {
						final var p = it.next();
						// Wenn der Typ unterschiedlich ist (z.B. int vs String), fliegt es raus
						if (!p.getValue().equals(currentProps.get(p.getKey()))) it.remove();
					}
				}
				// Early Exit: Wenn die Schnittmenge leer ist, müssen wir die restlichen Klassen nicht mehr prüfen
				if (commonProps.isEmpty()) break;
			}
			// Interface bauen und ausgeben
			final var out = new StringBuilder("public sealed interface ").append(interfaceName).append(" permits ").append(permits).append(" {");
			if (commonProps != null && !commonProps.isEmpty()) {
				out.append("\n");
				for (final var prop : commonProps.entrySet()) {
					out.append("    ").append(prop.getValue()).append(" ").append(prop.getKey()).append("();\n");
				}
			} else {
				out.append(" ");
			}
			final var java = out.append("}").toString();
			if (once.add(java)) System.out.println(java);
		}
	}

	public static int printJava(final Map<String, ObjectInfo> classes, final boolean showConflicts, final Map<String, Set<String>> sealedInterfaces) {
		var cnt = 0;
		for (final var oi : classes.values())
			if (oi.properties != null) for (final var propEntry : oi.properties.entrySet()) collectSealedInterfaces(propEntry.getValue(), sealedInterfaces);

		for (final var entry : classes.entrySet()) {
			final var oi = entry.getValue();
			final var clazz = oi.className;
			if (oi.hasConflict != showConflicts || clazz == null || "<MAP>".equals(clazz)) continue;
			if (oi.properties != null) cnt += oi.printRecord(sealedInterfaces);
		}
		return cnt;
	}

	public static final class ObjectInfo {
		public final String                    className;
		private      boolean                   hasConflict = false;
		public final Map<String, PropertyInfo> properties = new TreeMap<>();
		public ObjectInfo(final String classname) { this.className = classname; }

		public void setHasConflict() { hasConflict = true; }
		public boolean hasConflict() { return hasConflict; }

		int getMaxUseCount() {
			var count = 0;
			for (final var p : this.properties.values()) if(p.useCount > count) count = p.useCount;
			return count;
		}

		int printRecord(final Map<String, Set<String>> sealedInterfaces) {
			var count = getMaxUseCount();
			for (final var p : this.properties.values()) if(p.useCount > count) count = p.useCount;
			var idx = 0;
			final var pi = this.properties.entrySet().iterator();
			final var out = new StringBuilder(RECORD_PREFIX).append(this.className).append("(");
			switch(this.properties.size()) {
			case 0  -> out.append("/* FLAG record */");
			case 1  -> out.append(field(pi.next(), count));
			case 2  -> out.append(field(pi.next(), count)+", "+field(pi.next(), count));
			case 3  -> out.append(field(pi.next(), count)+", "+field(pi.next(), count)+", "+field(pi.next(), count));
			default -> { while(pi.hasNext()) out.append(idx++>0?",":" ").append("    ").append(field(pi.next(), count)); }
			}
			out.append(") ");
			mayImplements(out, className, sealedInterfaces);
			final var java =out.append(" { }").toString();
			final var fresh = once.add(java);
			if(fresh) System.out.println(java);
			return fresh?1:0;
		}
	}

	public static void main(final String[] argc) throws Throwable {
		final var bytes = Files.readAllBytes(Path.of("/1.json"));
		final var myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		Object json;
		try(var s = myEngine.jsonInputStream(new ByteArrayInputStream(bytes))) { json = s.readObject(Object.class); }
		final var enums            = new HashMap<String, Set<String>>();
		final var classes          = new HashMap<String, ObjectInfo>();
		final var info             = typeInfo(classes, enums, json);
		final var sealedInterfaces = new HashMap<String, Set<String>>() {

			@Override
			public Set<String> put(final String key, final Set<String> value) {
				if(key.isBlank()) throw new IllegalStateException();
				return super.put(key, value);
			}

		};
		System.out.println("INFO "+info.size()+" entries");
		System.out.println("WithoutConflict: "+printJava(classes, false, sealedInterfaces));
		System.out.println("WithConflict:    "+printJava(classes, true , sealedInterfaces));

		// System.out.println("WithConflict:    "+printJava(info   , false , sealedInterfaces));

		printEnums(enums, sealedInterfaces);
		System.out.println("IF "+sealedInterfaces.size());
		printInterfaces(classes, sealedInterfaces);
	}
}