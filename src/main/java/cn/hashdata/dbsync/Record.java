package cn.hashdata.dbsync;

/**
 * {@code Record} is the origin change data from data source.
 * 
 * @author yuze
 *
 */
public interface Record {
  /**
   * Get the field value for a given field.
   * 
   * @param fieldName the given field
   * @param oldValue only for update operation when primary key was updated, we need to get the old
   *        key and delete the old tuple
   *        
   * @return the value in string
   */
  String getField(String fieldName, boolean oldValue);
}
