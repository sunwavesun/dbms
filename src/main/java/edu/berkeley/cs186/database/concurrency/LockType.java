package edu.berkeley.cs186.database.concurrency;

import java.util.Arrays;
import java.util.List;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX; // shared intention exclusive

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible. A null represents no lock.
     */
    public static boolean compatible(LockType a, LockType b) {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted. A null
     * represents no lock.
     */
    public static LockType parentLock(LockType a) {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do). A null represents no lock.
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
};

