package nherald.indigo.store;

import java.util.Collection;

public interface StoreListOps
{
    /**
     * List the items in a particular namespace
     * @param namespace namespace
     * @return ids of all items in the namespace
     */
    Collection<String> list(String namespace);
}
