/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code AbstractCommitCallback} is an implements of {@code CommitCallback}, it can only commit
 * after all corresponding {@link ChangeLoader} finish their task.
 *
 * @author yuze
 */
public abstract class AbstractCommitCallback implements CommitCallback {
    protected int numOfTables;
    protected AtomicInteger numOfCommitedTables;
    protected AtomicBoolean committed;
    protected AtomicBoolean ready;
    protected long newestRecord; // the produce time of newest record

    public AbstractCommitCallback() {
        this.numOfCommitedTables = new AtomicInteger();
        this.committed = new AtomicBoolean();
        this.committed.set(false);
        this.ready = new AtomicBoolean();
        this.ready.set(false);
    }

    @Override
    public void setNumOfTables(int tables) {
        this.numOfTables = tables;
    }

    @Override
    public void done() {
        if (numOfCommitedTables.incrementAndGet() == numOfTables) {
            ready.set(true);
        }
    }

    @Override
    public boolean ready() {
        return ready.get();
    }

    @Override
    public synchronized void setNewestRecord(Long time) {
        if (time > newestRecord) {
            newestRecord = time;
        }
    }

    @Override
    public void destory() {
        numOfCommitedTables = null;
        committed = null;
        ready = null;
    }
}
