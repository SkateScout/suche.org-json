package org.suche.json.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.suche.json.JsonEngine;

class JsonOutputStreamEdgeCaseTest {

	@BeforeAll
	static void setupEngine() {
		// Dummy- oder Reale-Engine-Instanz holen (je nach deiner vorhandenen Architektur)
	}

	private static String serialize(final Object obj) {
		return JsonEngine.toString(obj);
	}

	@ParameterizedTest(name = "Edge Case Array/Collection: {0}")
	@MethodSource("provideEdgeCases")
	void testArrayAndCollectionEdgeCases(final Object input, final String expectedJson) {
		assertEquals(expectedJson, serialize(input));
	}

	private static Stream<Arguments> provideEdgeCases() {
		return Stream.of(
				// Der fixte Bug: Leere Objekt- und String-Arrays in Maps/Objekten
				Arguments.of(Map.of("a", new String[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new Object[]{}), "{\"a\":[]}"),

				// Primitive Arrays (Leer vs. Inhalt)
				Arguments.of(Map.of("a", new boolean[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new boolean[]{true, false}), "{\"a\":[true,false]}"),

				Arguments.of(Map.of("a", new int[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new int[]{1, 2}), "{\"a\":[1,2]}"),

				Arguments.of(Map.of("a", new long[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new long[]{10L}), "{\"a\":[10]}"),

				Arguments.of(Map.of("a", new double[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new double[]{1.5}), "{\"a\":[1.5]}"),

				Arguments.of(Map.of("a", new float[]{}), "{\"a\":[]}"),
				Arguments.of(Map.of("a", new short[]{}), "{\"a\":[]}"),

				// Standard Collections leer
				Arguments.of(Map.of("a", List.of()), "{\"a\":[]}"),
				Arguments.of(Map.of("a", Collections.emptyMap()), "{\"a\":[]}")
				);
	}

	@Test
	void testNestedEmptyStructures() {
		// Verschachtelte leere Strukturen testen, um Comma-Handling zu validieren
		final var nested = Map.of(
				"emptyArray", new String[]{},
				"filledArray", new int[]{1},
				"emptyBool", new boolean[]{}
				);

		final var result = serialize(nested);

		// Da Map.of keine garantierte Reihenfolge hat, prüfen wir auf die korrekten Fragmente
		// Jedes leere Array MUSS korrekt als [] geschlossen sein, kein Fragment darf "]" isoliert haben.
		final var faultPattern = java.util.regex.Pattern.compile(":\\]");
		org.junit.jupiter.api.Assertions.assertFalse(
				faultPattern.matcher(result).find(),
				"Malformed JSON found containing text ':]': " + result
				);
	}
}