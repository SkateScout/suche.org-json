#!/bin/bash

TARGET_DIR=${1:-.}
echo "Rollback: Stelle org.json wieder her in: $TARGET_DIR"

# 1. API-Aufrufe zurücksetzen (JSONObject)
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.textToJSONObject\((.+?)\)/new JSONObject($1)/g' {} +
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.byteToJSONObject\((.+?)\)/new JSONObject(new String($1))/g' {} +
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.pathToJSONObject\((.+?)\)/new JSONObject(Files.readString($1))/g' {} +

# 2. API-Aufrufe zurücksetzen (JSONArray)
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.textToJSONArray\((.+?)\)/new JSONArray($1)/g' {} +
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.byteToJSONArray\((.+?)\)/new JSONArray(new String($1))/g' {} +
# find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/JsonEngine\.pathToJSONArray\((.+?)\)/new JSONArray(Files.readString($1))/g' {} +

# 3. Importe und Typen zurücksetzen
find "$TARGET_DIR" -name "*.java" -type f -exec perl -pi -e 's/org\.suche\.json\.(JSONObject|JSONArray)/org.json.$1/g' {} +

echo "Rollback abgeschlossen."
