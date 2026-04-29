package org.suche.json.typeGenerator;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

final class ObjectInfo {
	final String className;

	boolean hasConflict = false;
	final Map<String, PropertyInfo> properties = new TreeMap<>();

	ObjectInfo(final String name) { this.className = name; }
	int getMaxUseCount() { return properties.values().stream().mapToInt(p -> p.useCount).max().orElse(0); }

	private static void mapInfo(final Map<String, ObjectInfo> classes, final Map<String, ObjectInfo> info, final String parentPath, final Map<?,?> m, final Map<String, Set<String>> enums, final Queue<Map.Entry<String, Object>> todo) {
		@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
		var name = "<MAP>";
		if (t.get(PropertyInfo.CLASS_KEY) instanceof final String s) name = s;
		else if (t.get(PropertyInfo.ENUM_KEY) instanceof final String s) name = s;
		final var path = parentPath + "/" + name;
		final var finalName = name;
		final var localObject = info.computeIfAbsent(path, _->new ObjectInfo(finalName));
		final var globalObject = classes.computeIfAbsent(name, _->new ObjectInfo(finalName));
		for(final var prop : t.entrySet()) {
			final var childName = prop.getKey();
			final var childType = prop.getValue();
			if(PropertyInfo.CLASS_KEY.equals(childName) || PropertyInfo.ENUM_KEY.equals(childName)) continue;
			final var localProp = localObject.properties.computeIfAbsent(childName, PropertyInfo::new);
			if(localProp.register(childName, enums, childType)) todo.add(Map.entry(path + "/" + childName, childType));
			final var globalProp = globalObject.properties.computeIfAbsent(childName, PropertyInfo::new);
			globalProp.register(childName, enums, childType);
		}
	}

	static Map<String,ObjectInfo> typeInfo(final Map<String, ObjectInfo> classes, final Map<String, Set<String>> enums, final Object root) {
		final var objectInfo = new HashMap<String,ObjectInfo>();
		final var todo = new ArrayDeque<Map.Entry<String, Object>>();
		final var localOnce = Collections.newSetFromMap(new IdentityHashMap<>());
		todo.add(Map.entry("", root));
		while(!todo.isEmpty()) {
			final var stack = todo.remove();
			final var value = stack.getValue();
			if(!localOnce.add(value)) continue;
			if (value instanceof final Map<?,?> m) mapInfo(classes, objectInfo, stack.getKey(), m, enums, todo);
			else if (value instanceof final Collection<?> t) {
				for(final var e : t) if (e instanceof Map || e instanceof Collection) todo.add(Map.entry(stack.getKey() + "[]", e));
			}
		}
		return objectInfo;
	}
}