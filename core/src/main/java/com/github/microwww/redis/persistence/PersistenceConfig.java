package com.github.microwww.redis.persistence;

import java.io.File;

public class PersistenceConfig {

    public static final String DEFAULT_DUMP_FILE_NAME = "dump.rdb";

    private boolean enabled = true;
    private File file = new File(DEFAULT_DUMP_FILE_NAME);
    private boolean autoLoad = true;
    private boolean saveOnClose = true;
    private long autoSaveIntervalMillis = 0;

    public PersistenceConfig() {
    }

    public PersistenceConfig(File file) {
        this.file = file;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public PersistenceConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public File getFile() {
        return file;
    }

    public PersistenceConfig setFile(File file) {
        this.file = file;
        return this;
    }

    public boolean isAutoLoad() {
        return autoLoad;
    }

    public PersistenceConfig setAutoLoad(boolean autoLoad) {
        this.autoLoad = autoLoad;
        return this;
    }

    public boolean isSaveOnClose() {
        return saveOnClose;
    }

    public PersistenceConfig setSaveOnClose(boolean saveOnClose) {
        this.saveOnClose = saveOnClose;
        return this;
    }

    public long getAutoSaveIntervalMillis() {
        return autoSaveIntervalMillis;
    }

    public PersistenceConfig setAutoSaveIntervalMillis(long autoSaveIntervalMillis) {
        this.autoSaveIntervalMillis = autoSaveIntervalMillis;
        return this;
    }
}
