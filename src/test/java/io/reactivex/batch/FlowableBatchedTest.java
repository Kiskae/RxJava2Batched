package io.reactivex.batch;

import io.reactivex.Flowable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.internal.functions.Functions;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class FlowableBatchedTest {
    Flowable<Integer> createFlowable(int batchSize, int maxToEmit) {
        return FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            long toEmit = Math.min(batchSize, requested);
            output.setSource(Flowable.range(state, (int) toEmit));

            if (state + toEmit >= maxToEmit) {
                output.terminalState();
            } else {
                output.setState(state + (int) toEmit);
            }
        }, Functions.emptyConsumer());
    }

    Flowable<Integer> createPaginatedFlowable(int batchSize, int maxToEmit) {
        return FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            long toEmit = Math.min(Math.min(batchSize, requested), maxToEmit - state);
            ConnectableFlowable<Integer> broadcastFlowable = Flowable.range(state, (int) toEmit).publish();

            // Begin emitting when source is subscribed to
            output.setSource(broadcastFlowable.autoConnect());

            if (state + toEmit >= maxToEmit) {
                output.terminalState();
            } else {
                // For the next state we begin at the last element + 1
                output.setState(broadcastFlowable.lastElement().map(i -> i + 1));
            }
        }, Functions.emptyConsumer());
    }

    @Test
    public void testSingleBatch() {
        Flowable<Integer> flowable = createFlowable(100, 100);

        flowable.test()
                .assertValueCount(100)
                .assertComplete();
    }

    @Test
    public void testMultipleBatch() {
        Flowable<Integer> flowable = createFlowable(50, 100);

        flowable.test()
                .assertValueCount(100)
                .assertComplete();
    }

    @Test
    public void testBackpressureOverrides() {
        Flowable<Integer> flowable = createFlowable(50, 1000);

        flowable.test(75)
                .assertValueCount(75)
                .assertNotTerminated()
                .dispose();
    }

    @Test
    public void testPaginatedBatch() {
        // 4 batches
        Flowable<Integer> flowable = createPaginatedFlowable(50, 200);

        flowable.test()
                .assertValueCount(200)
                .assertComplete();
    }

    @Test
    public void testRepeatedRequests() {
        // 4 batches
        Flowable<Integer> flowable = createPaginatedFlowable(50, 200);

        TestSubscriber<Integer> test = flowable.test(0);

        test.request(25);
        test.assertValueCount(25).assertNotComplete();

        test.request(50);
        test.assertValueCount(75).assertNotComplete();

        test.request(200);
        test.assertValueCount(200).assertComplete();
    }

    @Test
    public void testDisposesStateOnComplete() {
        AtomicBoolean disposeCalled = new AtomicBoolean(false);
        Flowable<Integer> flowable = FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            output.setSource(Flowable.just(state));
            output.setState(state + 1);
        }, (state) -> disposeCalled.set(true));

        flowable.take(0)
                .test()
                .assertComplete()
                .assertNoValues();

        assertTrue("disposeState not called onComplete", disposeCalled.get());
    }

    @Test
    public void testDisposesStateOnError() {
        AtomicBoolean disposeCalled = new AtomicBoolean(false);
        Flowable<Integer> flowable = FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            output.setSource(Flowable.error(new Exception()));
            output.setState(state + 1);
        }, (state) -> disposeCalled.set(true));

        flowable.take(1)
                .test()
                .assertNoValues()
                .assertTerminated()
                .assertNotComplete();

        assertTrue("disposeState not called onError", disposeCalled.get());
    }

    @Test
    public void testDisposesStateBasicCancel() {
        AtomicBoolean disposeCalled = new AtomicBoolean(false);
        Flowable<Integer> flowable = FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            output.setSource(Flowable.error(new Exception()));
            output.setState(state + 1);
        }, (state) -> disposeCalled.set(true));

        TestSubscriber<Integer> test = flowable.test(0);
        test.dispose();
        assertTrue(test.isDisposed());
        test.assertNotTerminated();

        assertTrue("disposeState not called through cancel", disposeCalled.get());
    }

    @Test
    public void testExternalLimiter() {
        Flowable<Integer> flowable = FlowableBatched.create(req -> Flowable.range(0, (int) Math.min(req, 100)));

        flowable.take(473).test().assertValueCount(473).assertComplete();
    }

    @Test
    public void testNoParallelExecution() throws InterruptedException {
        AtomicBoolean inGenerator = new AtomicBoolean(false);
        Flowable<Integer> flowable = FlowableBatched.create(Functions.justCallable(0), (state, requested, output) -> {
            output.setSource(Flowable.timer(1, TimeUnit.MILLISECONDS).map(i -> state).doOnSubscribe(x ->
                    assertTrue("Opening new source while original was still open.", inGenerator.compareAndSet(false, true))
            ).doOnTerminate(() ->
                    assertTrue("Closing unopened stream?", inGenerator.compareAndSet(true, false))
            ));
            output.setState(state + 1);
        }, Functions.emptyConsumer());

        flowable.take(5)
                .test()
                .await()
                .assertValueCount(5)
                .assertComplete();
    }
}
