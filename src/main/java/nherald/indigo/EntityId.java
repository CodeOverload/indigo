package nherald.indigo;

public class EntityId
{
    private final String namespace;
    private final String id;

    public EntityId(String namespace, String id)
    {
        this.namespace = namespace;
        this.id = id;
    }

    public String getNamespace()
    {
        return namespace;
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
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
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
        EntityId other = (EntityId) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("EntityId [id=").append(id).append(", namespace=").append(namespace).append("]");
        return builder.toString();
    }
}
