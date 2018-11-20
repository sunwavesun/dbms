package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.
    // The underlying lock manager.
    protected LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has. This is not the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity would still be the number of pages in the table.
    protected int capacity;

    // A cache of previously requested child contexts.
    protected Map<Object, LockContext> children;

    public LockContext(LockManager lockman, LockContext parent, Object name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Object name, boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.childLocksDisabled = readonly;
        this.readonly = readonly;
        this.numChildLocks = new HashMap<>();
        this.capacity = 0;
        this.children = new HashMap<>();
    }

    /**
     * Gets a lock context corresponding to NAME.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Object> names = name.getNames().iterator();
        LockContext ctx;
        Object n1 = names.next();
        if (n1.equals("database")) {
            ctx = lockman.databaseContext();
        } else {
            ctx = lockman.orphanContext(n1);
        }
        while (names.hasNext()) {
            ctx = ctx.childContext(names.next());
        }
        return ctx;
    }

    /**
     * Get the resource name that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION. Blocks the
     * transaction and places it in queue if the requested lock is not compatible
     * with another transaction's lock on the resource.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(BaseTransaction transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Release TRANSACTION's lock on NAME. Unblocks and dequeues all transactions
     * that can be unblocked, in order of lock request.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(BaseTransaction transaction)
    throws NoLockHeldException, InvalidLockException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. Blocks the transaction and places
     * TRANSACTION in the front of the queue if the request cannot be
     * immediately granted (i.e. another transaction holds a conflicting lock).
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(BaseTransaction transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Escalate TRANSACTION's lock from children of this context to this level, using
     * the least permissive lock necessary. There should be no child locks after this
     * call, and every operation valid on children of this context before this call
     * must still be valid. You should only make *one* call to the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock on children
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(BaseTransaction transaction) throws NoLockHeldException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Get the type of lock that TRANSACTION holds, or null if none. The lock type
     * returned should be the lock on this resource, or on the closest ancestor
     * that has a lock.
     */
    public LockType getGlobalLockType(BaseTransaction transaction) {
        if (transaction == null) {
            return null;
        }
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Get the type of lock that TRANSACTION holds, or null if no lock is held at this level.
     */
    public LockType getLocalLockType(BaseTransaction transaction) {
        if (transaction == null) {
            return null;
        }
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Disables locking children. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public LockContext childContext(Object name) {
        if (!this.children.containsKey(name)) {
            this.children.put(name, new LockContext(lockman, this, name, this.childLocksDisabled ||
                                                    this.readonly));
        }
        return this.children.get(name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity.
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(BaseTransaction transaction) {
        if (transaction == null || capacity == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity;
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

