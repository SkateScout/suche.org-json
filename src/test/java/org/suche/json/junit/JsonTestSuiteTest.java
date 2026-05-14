package org.suche.json.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.jupiter.api.Test;
import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

public class JsonTestSuiteTest {

	@Test
	public void testNstJsonTestSuite() throws Exception {
		// 1. ZIP-Datei im Temp-Verzeichnis zwischenspeichern (Cache)
		final var zipPath = Paths.get(System.getProperty("java.io.tmpdir"), "JSONTestSuite-master.zip");
		if (!Files.exists(zipPath)) {
			System.out.println("Lade NST JSONTestSuite herunter (einmalig)...");
			// Nutzt java.net.URI, da der URL-Konstruktor in neueren JDKs deprecated ist
			try (var in = URI.create("https://github.com/nst/JSONTestSuite/archive/refs/heads/master.zip").toURL().openStream()) {
				Files.copy(in, zipPath, StandardCopyOption.REPLACE_EXISTING);
			}
			System.out.println("Download abgeschlossen: " + zipPath);
		}

		int passedY = 0, passedN = 0, passedI = 0;
		var failed = 0;
		final var myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		myEngine.maxRecursiveDepth(0);
		// 2. Direktes Streamen aus dem ZIP (Zero-Disk-Extraction)
		try (var zis = new ZipInputStream(Files.newInputStream(zipPath))) {
			while (zis.getNextEntry() instanceof final ZipEntry entry) {
				if (entry.isDirectory()) continue;
				final var name = entry.getName();
				if (!name.contains("/test_parsing/") || !name.endsWith(".json")) continue;
				final var fileName = Paths.get(name).getFileName().toString();
				final var content  = zis.readAllBytes();
				try {
					try(var s = myEngine.jsonInputStream(content)) {
						if(Void.class  == s.readObject(Object.class)) throw new IllegalStateException();
					}

					// Wenn wir hier ankommen, hat der Parser keinen Fehler geworfen (Erfolg)
					if (fileName.startsWith("n_")) {
						System.err.println("FEHLER: Engine hätte ablehnen müssen -> " + fileName);
						failed++;
					} else if (fileName.startsWith("y_")) {
						passedY++;
					} else if (fileName.startsWith("i_")) {
						passedI++;
					}
				} catch (final Throwable e) {
					// Wenn wir hier ankommen, hat der Parser einen Fehler geworfen (Abbruch)
					if (fileName.startsWith("y_")) {
						System.err.println("FEHLER: Engine hätte akzeptieren müssen -> " + fileName + " | Grund: " + e.getMessage());
						e.printStackTrace();
						failed++;
					} else if (fileName.startsWith("n_")) {
						passedN++;
					} else if (fileName.startsWith("i_")) {
						passedI++;
					}
				}
			}
		}

		System.out.println("=========================================");
		System.out.printf("Ergebnisse der NST JSON Test Suite:%n");
		System.out.printf(" Korrekt akzeptiert (y_) : %d%n", passedY);
		System.out.printf(" Korrekt abgelehnt  (n_) : %d%n", passedN);
		System.out.printf(" Implementierungsabh. (i_): %d%n", passedI);
		System.out.println("=========================================");

		assertEquals(0, failed, failed + " Tests sind fehlgeschlagen! Siehe Konsolenausgabe für Details.");
	}
}