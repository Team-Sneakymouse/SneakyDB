package com.sneakydb.velocity

import com.sneakydb.DatabaseConfig
import org.spongepowered.configurate.yaml.YamlConfigurationLoader
import java.nio.file.Files
import java.nio.file.Path

/**
 * The proxy's `config.yml`: the same `database:` keys as Paper's, so operators can copy the values over.
 * Parsed with Configurate YAML, which velocity-api declares and the Velocity runtime ships.
 */
internal object ProxyConfig {
    const val FILE = "config.yml"
    private const val DEFAULT_RESOURCE = "/velocity-config.yml"

    /** Writes the bundled default into [directory] when no config exists yet, then reads `database:`. */
    fun load(directory: Path, info: (String) -> Unit): DatabaseConfig {
        val file = directory.resolve(FILE)
        if (Files.notExists(file)) {
            Files.createDirectories(directory)
            val default = checkNotNull(ProxyConfig::class.java.getResourceAsStream(DEFAULT_RESOURCE)) { "default config missing" }
            default.use { Files.copy(it, file) }
            info("Wrote default $file; set the database credentials and restart the proxy.")
        }
        val database = YamlConfigurationLoader.builder().path(file).build().load().node("database")
        return DatabaseConfig.read { key -> database.node(key).raw() }
    }
}
