package org.suche.json.typeGenerator;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

public final class Analyze {
	static final int    MIN_PREFIX_LEN  = 2;
	static final int    MIN_POSTIFX_LEN = 3;
	private static final int    PROPERTY_COUNT_THRESHOLD = 32;

	private static void optimizeDynamicMaps(final Map<String, ObjectInfo> classes) {
		final var iterator = classes.entrySet().iterator();
		while (iterator.hasNext()) {
			final var entry = iterator.next();
			final var className = entry.getKey();
			final var oi = entry.getValue();

			if (oi.properties == null || oi.properties.size() < PROPERTY_COUNT_THRESHOLD) continue;

			var firstType = -1;
			var allSame = true;
			PropertyInfo sampleProp = null;

			for (final var prop : oi.properties.values()) {
				if (firstType == -1) {
					firstType = prop.types;
					sampleProp = prop;
				} else if (firstType != prop.types) {
					allSame = false;
					break;
				}
			}

			if (allSame && sampleProp != null) {
				rewriteObjectToMapReferences(classes, className, sampleProp);
				iterator.remove();
			}
		}
	}

	private static void rewriteObjectToMapReferences(final Map<String, ObjectInfo> classes, final String targetClass, final PropertyInfo mapValueProp) {
		for (final var oi : classes.values()) {
			if (oi.properties == null) continue;
			for (final var prop : oi.properties.values()) {
				if ((prop.types & PropertyInfo.T_OBJECT) != 0 && prop.classes != null && prop.classes.contains(targetClass)) {
					prop.classes.remove(targetClass);
					prop.types &= ~PropertyInfo.T_OBJECT;
					prop.types |= PropertyInfo.T_MAP;
					if (prop.mapValues == null) {
						prop.mapValues = new PropertyInfo(prop.fieldName);
						prop.mapValues.types = mapValueProp.types;
						prop.mapValues.classes = mapValueProp.classes;
						prop.mapValues.arrayElements = mapValueProp.arrayElements;
					}
				}
			}
		}
	}

	static void collectSealedInterfaces(final PropertyInfo prop, final Map<String, Set<String>> sealedInterfaces, final Set<String> globalNames) {
		if (prop == null) return;
		final var allTypes = new TreeSet<String>();
		if (prop.classes != null) allTypes.addAll(prop.classes);
		if (prop.enumClasses != null) allTypes.addAll(prop.enumClasses);
		if (allTypes.size() > 1) {
			prop.interfaceName = PrettyNaming.findSealedInterfaceName(allTypes, prop.fieldName.replace("[]", ""), globalNames);
			sealedInterfaces.computeIfAbsent(prop.interfaceName, _->new TreeSet<>()).addAll(allTypes);
		}
		collectSealedInterfaces(prop.arrayElements, sealedInterfaces, globalNames);
		collectSealedInterfaces(prop.mapValues, sealedInterfaces, globalNames);
	}

	static void optimizeAutoEnums(final Map<String, ObjectInfo> classes, final Map<String, Set<String>> globalEnums, final Map<String, Set<String>> sealedInterfaces) {
		final var globalNames = new HashSet<>(classes.keySet());
		globalNames.addAll(globalEnums     .keySet());
		globalNames.addAll(sealedInterfaces.keySet());
		final var enumGroups = new HashMap<Set<String>, java.util.List<PropertyInfo>>();
		for (final var oi : classes.values()) {
			if (oi.properties == null) continue;
			for (final var prop : oi.properties.values()) {
				if (prop.useCount >= 2048 && !prop.autoStringAborted && prop.autoStringValues != null && !prop.autoStringValues.isEmpty()) {
					enumGroups.computeIfAbsent(prop.autoStringValues, _ -> new ArrayList<>()).add(prop);
				}
			}
		}

		for (final var entry : enumGroups.entrySet()) {
			final var values = entry.getKey();
			final var props  = entry.getValue();
			final var fieldNames = new TreeSet<String>();
			for (final var prop : props) fieldNames.add(prop.fieldName);
			final var enumName = PrettyNaming.findCommonName(fieldNames, globalNames);
			final var finalName = PrettyNaming.findUniqueName(enumName, globalNames);
			globalNames.add(finalName);
			globalEnums.put(finalName, new TreeSet<>(values));
			for (final var prop : props) {
				prop.types         = PropertyInfo.T_ENUM;
				prop.interfaceName = finalName;
			}
		}
	}

	private static Object readJson(final Path jsonPath) throws Throwable {
		final var myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		final var bytes = Files.readAllBytes(jsonPath);
		return myEngine.jsonInputStream(new ByteArrayInputStream(bytes)).readObject(Object.class);
	}

	public static void main(final String[] argc) throws Throwable {
		final var basePackageName = "org.suche.json.mode.testDataset1";
		final var srcPath = Path.of("src/generated/java");

		final var json = readJson(Path.of("/1.json"));

		final var enums   = new HashMap<String, Set<String>>();
		final var classes = new HashMap<String, ObjectInfo>();
		ObjectInfo.typeInfo(classes, enums, json);
		final var sealedInterfaces = new HashMap<String, Set<String>>();
		optimizeDynamicMaps(classes);
		optimizeAutoEnums(classes, enums, sealedInterfaces);
		final var globalNames = new HashSet<>(classes.keySet());
		globalNames.addAll(enums.keySet());

		for (final var oi : classes.values())
			if (oi.properties != null) for (final var p : oi.properties.values()) collectSealedInterfaces(p, sealedInterfaces, globalNames);

		if (json instanceof Collection || (json != null && json.getClass().isArray())) {
			final var rootProp = new PropertyInfo("root");
			rootProp.register("root", enums, json);
			collectSealedInterfaces(rootProp, sealedInterfaces, globalNames);
		}

		final var allGeneratedTypes = new HashSet<String>();
		for (final var oi : classes.values()) if (!oi.hasConflict && !"<MAP>".equals(oi.className)) allGeneratedTypes.add(oi.className);
		allGeneratedTypes.addAll(enums.keySet());
		allGeneratedTypes.addAll(sealedInterfaces.keySet());
		final var subPackages     = PrettyNaming.determineSubPackages(allGeneratedTypes);
		try(final var c = ModelCompiler.of(srcPath, basePackageName, subPackages)) {
			SourceGenerator.writeFSourceFiles(c, classes, enums, sealedInterfaces, basePackageName, subPackages);
		}
	}
}