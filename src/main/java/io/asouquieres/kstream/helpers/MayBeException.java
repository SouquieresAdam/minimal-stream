package io.asouquieres.kstream.helpers;

public class MayBeException<T> {
    public StreamException<T> streamException;
    public T streamValue;

    private MayBeException(T value) {
        this.streamValue = value;
    }

    private MayBeException(StreamException<T> ex) {
        this.streamException = ex;
    }

    public static <T> MayBeException<T> of(T v) {
        return new MayBeException<>(v);
    }

    public static <T> MayBeException<T> of(StreamException<T> ex) {
        return new MayBeException<>(ex);
    }
}
