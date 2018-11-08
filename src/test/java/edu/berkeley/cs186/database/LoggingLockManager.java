package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LoggingLockManager extends LockManager {
    public List<String> log = new ArrayList<>();
    private boolean logging = false;
    private boolean suppressInternal = true;

    @Override
    public LockContext databaseContext() {
        if (!contexts.containsKey("database")) {
            contexts.put("database", new LoggingLockContext(this, null, "database"));
        }
        return contexts.get("database");
    }

    @Override
    public LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LoggingLockContext(this, null, name));
        }
        return contexts.get(name);
    }

    @Override
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks) {
        boolean oldLogging = logging;
        logging = !suppressInternal;
        super.acquireAndRelease(transaction, name, lockType, releaseLocks);
        logging = oldLogging;
        emit("acquire/t " + transaction.getTransNum() + " " + name + " " + lockType);
        Collections.sort(releaseLocks, Comparator.comparing(ResourceName::toString));
        for (ResourceName n : releaseLocks) {
            emit("release/t " + transaction.getTransNum() + " " + n);
        }
    }

    @Override
    public void acquire(BaseTransaction transaction, ResourceName name, LockType type) {
        boolean oldLogging = logging;
        logging = !suppressInternal;
        super.acquire(transaction, name, type);
        logging = oldLogging;
        emit("acquire " + transaction.getTransNum() + " " + name + " " + type);
    }

    @Override
    public void release(BaseTransaction transaction, ResourceName name) {
        boolean oldLogging = logging;
        logging = !suppressInternal;
        super.release(transaction, name);
        logging = oldLogging;
        emit("release " + transaction.getTransNum() + " " + name);
    }

    @Override
    public void promote(BaseTransaction transaction, ResourceName name, LockType newLockType) {
        boolean oldLogging = logging;
        logging = !suppressInternal;
        super.promote(transaction, name, newLockType);
        logging = oldLogging;
        emit("promote " + transaction.getTransNum() + " " + name + " " + newLockType);
    }

    public void startLog() {
        logging = true;
    }

    public void endLog() {
        logging = false;
    }

    public void clearLog() {
        log.clear();
    }

    public boolean isLogging() {
        return logging;
    }

    public void suppressInternals(boolean toggle) {
        suppressInternal = toggle;
    }

    public void emit(String s) {
        if (logging) {
            log.add(s);
            // System.out.println(s);
        }
    }
}