package com.sneakydb;

/** Stable entry point for consumer plugins; implementation and driver types stay private to SneakyDB. */
public final class SneakyDBProvider {
    private static volatile SneakyDBApi api;

    private SneakyDBProvider() {}

    public static SneakyDBApi getApi() {
        SneakyDBApi current = api;
        if (current == null) throw new IllegalStateException("SneakyDB is not loaded");
        return current;
    }

    static void install(SneakyDBApi value) { api = value; }
    static void clear() { api = null; }
}
