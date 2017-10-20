package io.confluent.examples.streams.microservices.util;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StubAsyncResponse implements AsyncResponse {

    public Object response;

    @Override
    public boolean resume(Object o) {
        this.response = o;
        return false;
    }

    @Override
    public boolean resume(Throwable throwable) {
        return false;
    }

    @Override
    public boolean cancel() {
        return false;
    }

    @Override
    public boolean cancel(int i) {
        return false;
    }

    @Override
    public boolean cancel(Date date) {
        return false;
    }

    @Override
    public boolean isSuspended() {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    @Override
    public boolean setTimeout(long l, TimeUnit timeUnit) {
        return false;
    }

    @Override
    public void setTimeoutHandler(TimeoutHandler timeoutHandler) {

    }

    @Override
    public Collection<Class<?>> register(Class<?> aClass) {
        return null;
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Class<?> aClass, Class<?>... classes) {
        return null;
    }

    @Override
    public Collection<Class<?>> register(Object o) {
        return null;
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Object o, Object... objects) {
        return null;
    }
}
