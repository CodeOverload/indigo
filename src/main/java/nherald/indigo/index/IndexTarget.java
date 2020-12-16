package nherald.indigo.index;

import nherald.indigo.Entity;

/**
 * Defines what should be indexed. This could be a field or a combination
 * of fields from the source object
 */
@FunctionalInterface
public interface IndexTarget<T extends Entity>
{
    public String getTextFromEntity(T entity);
}
