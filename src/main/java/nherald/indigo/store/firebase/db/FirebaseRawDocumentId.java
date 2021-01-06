package nherald.indigo.store.firebase.db;

public class FirebaseRawDocumentId
{
    private final String collection;
    private final String id;

    public FirebaseRawDocumentId(String collection, String id)
    {
        this.collection = collection;
        this.id = id;
    }

    public String getCollection()
    {
        return collection;
    }

    public String getId()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((collection == null) ? 0 : collection.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FirebaseRawDocumentId other = (FirebaseRawDocumentId) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (collection == null) {
            if (other.collection != null)
                return false;
        } else if (!collection.equals(other.collection))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("FirebaseDocumentId [id=").append(id).append(", collection=").append(collection).append("]");
        return builder.toString();
    }
}
