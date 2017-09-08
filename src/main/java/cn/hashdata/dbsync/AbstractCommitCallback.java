package cn.hashdata.dbsync;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code AbstractCommitCallback} is an implements of {@code CommitCallback}, it can only commit
 * after all corresponding loader finish loading their task.
 *
 * @author yuze
 *
 */
public abstract class AbstractCommitCallback implements CommitCallback {
  protected int numOfTables;
  protected AtomicInteger numOfCommitedTables;
  protected AtomicBoolean committed;
  protected AtomicBoolean ready;

  public AbstractCommitCallback() {
    this.numOfCommitedTables = new AtomicInteger();
    this.committed = new AtomicBoolean();
    this.ready = new AtomicBoolean();
    this.ready.set(false);
  }

  @Override
  public void setNumOfTables(int tables) {
    this.numOfTables = tables;
  }

  /**
   * Commit a successful load task for a table.
   */
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
}
