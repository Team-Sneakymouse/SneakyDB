package com.sneakydb

/** Stable entry point for consumer plugins; implementation and driver types stay private to SneakyDB. */
object SneakyDBProvider {
    /** Returns the API after SneakyDB has loaded. */
    @JvmStatic
    fun getApi(): SneakyDBApi =
        SneakyDBRegistry.api ?: throw IllegalStateException("SneakyDB is not loaded")
}

internal object SneakyDBRegistry {
    @Volatile
    var api: SneakyDBApi? = null
}
