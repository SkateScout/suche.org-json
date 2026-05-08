#!/bin/bash

TARGET_DIR=${1:-.}
echo "Starte erweiterte Migration von org.json zu org.suche.json in: $TARGET_DIR"

# 1. Spezifische Konstruktoren auflösen (JSONObject)
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONObject\(\s*new\s+JSONTokenerChars\((.+?)\)\s*\)/JsonEngine.textToJSONObject($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONObject\(\s*new\s+String\(([^,]+)\s*,\s*0\s*,\s*\1\.length\)\s*\)/JsonEngine.byteToJSONObject($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONObject\(\s*new\s+String\((.+?)\)\s*\)/JsonEngine.byteToJSONObject($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONObject\(\s*Files\.readString\((.+?)\)\s*\)/JsonEngine.pathToJSONObject($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JSONObject\.of\((.+?)\)/JsonEngine.pathToJSONObject($1)/g' {} +

# 2. Spezifische Konstruktoren auflösen (JSONArray)
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONArray\(\s*new\s+JSONTokenerChars\((.+?)\)\s*\)/JsonEngine.textToJSONArray($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONArray\(\s*new\s+String\(([^,]+)\s*,\s*0\s*,\s*\1\.length\)\s*\)/JsonEngine.byteToJSONArray($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONArray\(\s*new\s+String\((.+?)\)\s*\)/JsonEngine.byteToJSONArray($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONArray\(\s*Files\.readString\((.+?)\)\s*\)/JsonEngine.pathToJSONArray($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JSONArray\.of\((.+?)\)/JsonEngine.pathToJSONArray($1)/g' {} +

# 3. Allgemeine Konstruktoren auflösen (ohne Komma innerhalb der Klammer, um Methodenaufrufe mit Maps zu überspringen)
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONObject\(([^,)]+)\)/JsonEngine.textToJSONObject($1)/g' {} +
#find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/new\s+JSONArray\(([^,)]+)\)/JsonEngine.textToJSONArray($1)/g' {} +

# 4. Importe und Typen anpassen
find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/org\.json\.(JSONObject|JSONArray)/org.suche.json.$1/g' {} +

echo "Migration abgeschlossen. Bitte Projekt kompilieren und eventuelle Map-Konstruktoren manuell prüfen."
