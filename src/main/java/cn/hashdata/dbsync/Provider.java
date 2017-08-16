package cn.hashdata.dbsync;

/**
 * {@code Provider} is responsible for polling data from data source and provide to
 * {@code Dispatcher}. Each {@code Provider} must have a unique name and maintain a pool of
 * {@code Transformer}, which could transform the polled data to dbsync inner format.
 *
 * @author yuze
 *
 */
public interface Provider {
  /**
   *
   * @return the name of this {@code Provider}.
   */
  public String getProviderName();

  /**
   * Borrow a {@code Transformer} from the maintained pool. This method should be non-blocking. If
   * no {@code Transformer} is available currently, create a new {@code Transformer}.
   *
   * @param changeSet The {@code ChangeSet} that need to be transformed.
   * @return The borrowed {@code Transformer}.
   */
  public Transformer borrowTransformer(ChangeSet changeSet);

  /**
   * Return the borrowed {@code Transformer} to the pool.
   * @param trans The {@code Transformer} should be returned.
   */
  public void returnTransformer(Transformer trans);
}
