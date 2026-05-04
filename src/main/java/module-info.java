module org.suche.json {
	requires java.logging;
	requires java.compiler;
	requires static jdk.incubator.vector;	// Optional dependend on runtime
	exports org.suche.json;
}