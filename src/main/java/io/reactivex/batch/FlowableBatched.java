package io.reactivex.batch;

import io.reactivex.Flowable;
import io.reactivex.MaybeObserver;
import io.reactivex.MaybeSource;
import io.reactivex.annotations.BackpressureKind;
import io.reactivex.annotations.BackpressureSupport;
import io.reactivex.batch.internal.FlowableBatchedGenerator;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.internal.functions.Functions;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.plugins.RxJavaPlugins;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.Callable;

/**
 *
 */
public class FlowableBatched {
    // Used when state is not used, since the state variable cannot be NULL
    private static final Callable<Object> LACK_OF_STATE = Functions.justCallable(new Object());

    public interface BatchResource<T, S> {
        /**
         * Sets the publisher that will emit the next set of values.
         * <p>
         * Events sent to {@link Subscriber#onNext(Object)} and {@link Subscriber#onError(Throwable)} will
         * be forwarded to the outer {@link Flowable} while {@link Subscriber#onComplete()} will cause another
         * call to {@link BatchGenerator#generate(Object, long, BatchResource)}.
         *
         * @param publisher value publisher
         * @throws IllegalArgumentException if this method was already invoked once.
         */
        void setSource(Publisher<? extends T> publisher);

        /**
         * Sets the given source as determining the next state provided to
         * {@link BatchGenerator#generate(Object, long, BatchResource)}.
         * <p>
         * * If it does not produce anything ({@link MaybeObserver#onComplete()}) then the outer {@link Flowable} will
         * end.
         * * If it produces a value ({@link MaybeObserver#onSuccess(Object)} that value will be used as the
         * next state.
         * * If it produces an error ({@link MaybeObserver#onError(Throwable)}) that error will be forwarded to
         * the outer {@link Flowable}.
         *
         * @param stateSource source of the next state
         * @throws IllegalArgumentException if one of the *State methods was already invoked once.
         */
        void setState(MaybeSource<S> stateSource);

        /**
         * Sets the value as the next state provided to {@link BatchGenerator#generate(Object, long, BatchResource)}.
         *
         * @param state the next state
         * @throws IllegalArgumentException if one of the *State methods was already invoked once.
         */
        void setState(S state);

        /**
         * Indicates this is the terminal state and the outer {@link FlowableBatched} will terminate after the publisher
         * set with {@link #setSource(Publisher)} terminates.
         *
         * @throws IllegalArgumentException if one of the *State methods was already invoked once.
         */
        void terminalState();
    }

    public interface BatchGenerator<T, S> {
        /**
         * Called when the outer {@link Flowable} requires more elements.
         * <p>
         * The implementation must always call {@link BatchResource#setSource(Publisher)} for each invocation with a
         * source of elements to emit.
         * <p>
         * Calling one of the *State methods is optional and by default it will re-use {@code state}.
         *
         * @param state     Previous state as supplied by the user.
         * @param requested Outstanding number of requested elements.
         * @param output    Used to signal the next response from this stream.
         * @throws Exception on error
         */
        void generate(S state, long requested, BatchResource<T, S> output) throws Exception;
    }

    /**
     * @param generator function which produces a publisher given a request for values.
     * @param <T>       Type of items emitted by this {@link Flowable}
     * @return the new Flowable instance
     */
    @BackpressureSupport(value = BackpressureKind.FULL)
    public static <T> Flowable<T> create(final Function<Long, Publisher<? extends T>> generator) {
        ObjectHelper.requireNonNull(generator, "generator is null");

        return create(LACK_OF_STATE, new BatchGenerator<T, Object>() {
            @Override
            public void generate(Object state, long requested, BatchResource<T, Object> output) throws Exception {
                output.setSource(generator.apply(requested));
            }
        }, Functions.emptyConsumer());
    }

    @BackpressureSupport(value = BackpressureKind.FULL)
    public static <T, S> Flowable<T> create(Callable<S> initialState,
                                            BatchGenerator<T, S> generator) {
        return create(initialState, generator, Functions.<S>emptyConsumer());
    }

    @BackpressureSupport(value = BackpressureKind.FULL)
    public static <T, S> Flowable<T> create(Callable<S> initialState,
                                            BatchGenerator<T, S> generator,
                                            Consumer<? super S> disposeState) {
        ObjectHelper.requireNonNull(initialState, "initialState is null");
        ObjectHelper.requireNonNull(generator, "generator is null");
        ObjectHelper.requireNonNull(disposeState, "disposeState is null");

        return RxJavaPlugins.onAssembly(new FlowableBatchedGenerator<T, S>(initialState, generator, disposeState));
    }
}
