package org.suche.json.typeGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.lang.model.SourceVersion;

final class PrettyNaming {
	static boolean isValidVariableName(final CharSequence name) {
		return javax.lang.model.SourceVersion.isIdentifier(name) && !SourceVersion.isKeyword(name);
	}

	static Map<String, String> determineSubPackages(final Set<String> classNames) {
		final var suffixCounts = new HashMap<String, Integer>();
		final var prefixCounts = new HashMap<String, Integer>();
		final var subPackages  = new HashMap<String, String>();
		for (final var name : classNames) {
			final var parts = splitCamelCase(name);
			if (parts.size() >= 2) {
				final var first = parts.getFirst();
				final var last = parts.getLast();
				if (first.length() > 3) prefixCounts.merge(first, 1, Integer::sum);
				if (last.length() > 3) suffixCounts.merge(last, 1, Integer::sum);
			}
		}
		for (final var name : classNames) {
			final var parts = splitCamelCase(name);
			if (parts.size() >= 2) {
				final var first = parts.getFirst();
				final var last = parts.getLast();
				if      (suffixCounts.getOrDefault(last , 0) > 5)  subPackages.put(name, last.toLowerCase());
				else if (prefixCounts.getOrDefault(first, 0) > 5)  subPackages.put(name, first.toLowerCase());
				else                                               subPackages.put(name, "");
			} else                                                 subPackages.put(name, "");
		}
		return subPackages;
	}

	private static List<String> splitCamelCase(final String s) {
		final var result = new java.util.ArrayList<String>();
		var start = 0;
		for (var i = 1; i < s.length(); i++) {
			if (Character.isUpperCase(s.charAt(i))) {
				result.add(s.substring(start, i));
				start = i;
			}
		}
		result.add(s.substring(start));
		return result;
	}

	private static int commonPrefixLength(final String first, final String next, final int preLen) {
		if(preLen < Analyze.MIN_PREFIX_LEN) return preLen;
		var i = 0;
		while (i < Math.min(preLen, next.length()) && first.charAt(i) == next.charAt(i)) i++;
		return i;
	}

	private static int commonPostfixLength(final String first, final String next, final int postLen) {
		if(postLen < Analyze.MIN_POSTIFX_LEN) return postLen;
		var i = 0;
		while (i < Math.min(postLen, next.length()) && first.charAt(first.length()-1-i) == next.charAt(next.length()-1-i)) i++;
		return i;
	}

	static String findCommonName(final Set<String> fieldNames, final Set<String> globalNames) {
		if (fieldNames == null || fieldNames.isEmpty()) return "AutoEnum";
		final var iterator = fieldNames.iterator();
		final var first = iterator.next();
		String result;
		if (fieldNames.size() == 1) {
			result = Character.toUpperCase(first.charAt(0)) + first.substring(1);
		} else {
			var preLen  = first.length();
			var postLen = first.length();
			while (iterator.hasNext()) {
				final var next = iterator.next();
				preLen  = commonPrefixLength (first, next, preLen);
				postLen = commonPostfixLength(first, next, postLen);
			}
			if (preLen >= Analyze.MIN_PREFIX_LEN && postLen >= Analyze.MIN_POSTIFX_LEN)
				result = Character.toUpperCase(first.charAt(0))  + first.substring(1, preLen)
				+        Character.toUpperCase(first.charAt(first.length()-postLen)) + first.substring(first.length()-postLen+1);
			else if (preLen >= Analyze.MIN_PREFIX_LEN)
				result = Character.toUpperCase(first.charAt(0))  + first.substring(1, preLen);
			else if (postLen >= Analyze.MIN_POSTIFX_LEN)
				result = Character.toUpperCase(first.charAt(first.length()-postLen)) + first.substring(first.length()-postLen+1);
			else  result = "SharedEnum";
		}
		if(globalNames != null) globalNames.add(result = findUniqueName(result, globalNames));
		return result;
	}

	static String findUniqueName(final String name, final Set<String> globalNames) {
		if (globalNames == null || !globalNames.contains(name)) return name;
		for(var i = 0;i < 1000; i++) if (!globalNames.contains(name+i)) return name+i;
		throw new IllegalStateException();
	}

	static String findSealedInterfaceName(final Set<String> classes, final String fieldName, final Set<String> globalNames) {
		if (classes == null || classes.isEmpty()) return "Object";
		final var iterator = classes.iterator();
		final var first = iterator.next();
		var preLen = first.length();
		var postLen = first.length();
		while (iterator.hasNext()) {
			final var next = iterator.next();
			preLen  = commonPrefixLength (first, next, preLen);
			postLen = commonPostfixLength(first, next, postLen);
		}

		String result;
		if (preLen >= Analyze.MIN_PREFIX_LEN && postLen >= Analyze.MIN_POSTIFX_LEN) {
			result = first.substring(0, preLen) + Character.toUpperCase(first.charAt(first.length()-postLen)) + first.substring(first.length()-postLen+1);
		} else if (preLen >= Analyze.MIN_PREFIX_LEN) {
			result = first.substring(0, preLen);
		} else if (postLen >= Analyze.MIN_POSTIFX_LEN) {
			result = Character.toUpperCase(first.charAt(first.length()-postLen)) + first.substring(first.length()-postLen+1);
		} else {
			final var isGeneric = fieldName == null || fieldName.isEmpty() || "root".equals(fieldName) || "Element".equals(fieldName);
			final var baseName = isGeneric ? first : fieldName;
			if(null == baseName) throw new IllegalStateException();
			result = Character.toUpperCase(baseName.charAt(0)) + baseName.substring(1);
			if (isGeneric && !result.endsWith("Base") && !result.startsWith("I")) result += "Base";
		}

		if (classes.contains(result) || (globalNames != null && globalNames.contains(result))) result = "I" + result;
		return result;
	}
}