package io.reactivex.batch.internal;

import io.reactivex.*;
import io.reactivex.batch.FlowableBatched;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.SerialDisposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.subscriptions.EmptySubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.AtomicThrowable;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.internal.util.ExceptionHelper;
import io.reactivex.plugins.RxJavaPlugins;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FlowableBatchedGenerator<T, S> extends Flowable<T> {
    final Callable<S> stateSupplier;
    final FlowableBatched.BatchGenerator<T, S> generator;
    final Consumer<? super S> disposeState;

    public FlowableBatchedGenerator(Callable<S> stateSupplier,
                                    FlowableBatched.BatchGenerator<T, S> generator,
                                    Consumer<? super S> disposeState) {
        this.stateSupplier = stateSupplier;
        this.generator = generator;
        this.disposeState = disposeState;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        S state;

        try {
            state = stateSupplier.call();
        } catch (Throwable e) {
            Exceptions.throwIfFatal(e);
            EmptySubscription.error(e, s);
            return;
        }

        s.onSubscribe(new BatchGeneratorSubscription<T, S>(s, generator, disposeState, state));
    }

    static class ResourceHolder<T, S> implements FlowableBatched.BatchResource<T, S> {
        Publisher<? extends T> publisher;
        MaybeSource<S> stateSource;

        @Override
        public void setSource(Publisher<? extends T> publisher) {
            if (this.publisher != null) {
                throw new IllegalStateException("Do not call setSource twice within generate()");
            }
            this.publisher = ObjectHelper.requireNonNull(publisher, "publisher is null");
        }

        @Override
        public void setState(MaybeSource<S> stateSource) {
            if (this.stateSource != null) {
                throw new IllegalStateException("Do not call *State methods twice within generate()");
            }
            this.stateSource = ObjectHelper.requireNonNull(stateSource, "stateSource is null");
        }

        @Override
        public void setState(S state) {
            setState(Maybe.just(state));
        }

        @Override
        public void terminalState() {
            setState(Maybe.<S>empty());
        }

        void reset() {
            this.publisher = null;
            this.stateSource = null;
        }
    }

    static class BatchGeneratorSubscription<T, S> extends AtomicLong
            implements Subscription, MaybeObserver<S> {
        final Subscriber<? super T> actual;
        final FlowableBatched.BatchGenerator<T, S> generator;
        final Consumer<? super S> disposeState;

        final AtomicReference<S> nextState;
        final ResourceHolder<T, S> holder = new ResourceHolder<T, S>();
        final AtomicBoolean readyToEmit = new AtomicBoolean(true);

        // Current inner subscription
        final AtomicReference<Subscription> innerSubscription = new AtomicReference<Subscription>();
        final SerialDisposable stateDisposable = new SerialDisposable();

        volatile boolean cancelled;
        final AtomicThrowable error;

        BatchGeneratorSubscription(Subscriber<? super T> actual,
                                   FlowableBatched.BatchGenerator<T, S> generator,
                                   Consumer<? super S> disposeState,
                                   S initialState) {
            this.actual = actual;
            this.generator = generator;
            this.disposeState = disposeState;
            this.nextState = new AtomicReference<S>(initialState);
            this.error = new AtomicThrowable();
        }

        final AtomicInteger wip = new AtomicInteger(0);

        /*
         * Since inner producers can be any type, this loop
         * ensures nested requests are flattened.
         */
        private void tryGenerate() {
            if (wip.getAndIncrement() == 0) {
                for (; ; ) {
                    if (cancelled) {
                        dispose(nextState.getAndSet(null));
                        return;
                    }

                    if (error.isTerminated()) {
                        cancelled = true;
                        dispose(nextState.getAndSet(null));
                        return;
                    }

                    long toRequest = get();
                    if (toRequest != 0) {
                        Subscription activeSubscription = innerSubscription.get();
                        if (activeSubscription != null) {
                            activeSubscription.request(toRequest);
                        } else {
                            generateNextBatch(toRequest);
                        }
                    }

                    // Check if we need to run tryGenerate() again
                    if (wip.decrementAndGet() == 0) return;
                }
            }
        }

        @Override
        public void request(long n) {
            if (!SubscriptionHelper.validate(n)) {
                return;
            }

            if (BackpressureHelper.add(this, n) != 0L) {
                return;
            }

            tryGenerate();
        }

        private void generateNextBatch(long toRequest) {
            if (!readyToEmit.compareAndSet(true, false)) {
                //TODO: figure out correct assertion checking method
                throw new AssertionError("generateNextBatch called whilst new readyToEmit");
            }

            final S state = nextState.getAndSet(null);

            if (state == null) {
                //TODO: figure out correct assertion checking method
                throw new AssertionError("generateNextBatch called while nextState held null");
            }

            holder.reset();
            try {
                generator.generate(state, toRequest, holder);
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                cancelled = true;
                actual.onError(ex);
                return;
            }

            if (holder.publisher == null) {
                actual.onError(new NullPointerException("setSource(...) not called in generate()"));
                return;
            }

            // Set up updating of state
            if (holder.stateSource == null) {
                onSuccess(state);
            } else {
                holder.stateSource.subscribe(this);
            }

            // Set up forwarding of values
            Subscriber<T> subscriber = new SourceSubscriber<T>(this);
            holder.publisher.subscribe(subscriber);
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;

                // Cancel the ongoing subscriptions, clean up references
                if (SubscriptionHelper.cancel(innerSubscription)) {
                    stateDisposable.dispose();
                }

                if (BackpressureHelper.add(this, 1) == 0) {
                    dispose(nextState.getAndSet(null));
                }
            }

        }

        private void dispose(S state) {
            if (state == null) return;

            try {
                disposeState.accept(state);
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                RxJavaPlugins.onError(ex);
            }
        }

        // State updating methods
        @Override
        public void onSubscribe(Disposable d) {
            this.stateDisposable.replace(d);
        }

        @Override
        public void onSuccess(S value) {
            if (!nextState.compareAndSet(null, value)) {
                //TODO: probably shouldn't go through onError..
                onError(new IllegalStateException("nextState held a value when it should not be holding one"));
            }

            // If the previous publisher finished (it set readyToEmit to true), emit the next batch
            if (!readyToEmit.compareAndSet(false, true)) {
                tryGenerate();
            }
        }

        @Override
        public void onError(Throwable e) {
            if (error.addThrowable(e)) {
                e = error.terminate();
                if (e != ExceptionHelper.TERMINATED) {
                    this.actual.onError(e);
                }
            } else {
                RxJavaPlugins.onError(e);
            }

            // Perform state cleanup
            tryGenerate();
        }

        @Override
        public void onComplete() {
            Throwable ex = error.terminate();
            if (ex != null && ex != ExceptionHelper.TERMINATED) {
                if (ex != ExceptionHelper.TERMINATED) {
                    this.actual.onError(ex);
                }
            } else {
                this.actual.onComplete();
            }

            // Perform state cleanup
            tryGenerate();
        }

        void innerOnSubscribe(Subscription s) {
            if (SubscriptionHelper.replace(innerSubscription, s)) {
                s.request(get());
            }
        }

        void produced(long elementsEmitted) {
            long toProduce = BackpressureHelper.produced(this, elementsEmitted);

            // Clean up the subscription
            if (!SubscriptionHelper.replace(innerSubscription, null)) return;

            // If the state is ready (it set readyToEmit to true), emit the next batch
            if (!readyToEmit.compareAndSet(false, true) && toProduce != 0) {
                tryGenerate();
            }
        }
    }

    static class SourceSubscriber<T> implements Subscriber<T> {
        final BatchGeneratorSubscription<T, ?> outerSubscription;
        long onNextCalled = 0;

        SourceSubscriber(BatchGeneratorSubscription<T, ?> outerSubscription) {
            this.outerSubscription = outerSubscription;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.outerSubscription.innerOnSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            if (t == null) {
                onError(new NullPointerException("onNext called with null. Null values are generally not allowed in 2.x operators and sources."));
                return;
            }

            // Safe since onNext calls are serialized
            onNextCalled++;
            this.outerSubscription.actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            this.outerSubscription.onError(t);
        }

        @Override
        public void onComplete() {
            outerSubscription.produced(onNextCalled);
        }
    }
}
