package nherald.indigo.store.firebase.db;

public interface FirebaseRawDocument
{
    boolean exists();

    <T> T asObject(Class<T> type);
}
