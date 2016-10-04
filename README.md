# RxJava2Batched
Implementation of a Flowable that allows batched requests.

### Examples:

Paginated by offset:

    private final static int RESULTS_PER_PAGE = 100;
    private Flowable<Data> getPage(Integer offset, Integer limit);
    
    Flowable<Data> stream() {
        return FlowableBatched.create(Functions.justCallable(0), (currentOffset, requested, output) -> {
            int toEmit = (int) Math.min(requested, RESULTS_PER_PAGE);
            ConnectableFlowable<Data> data = getPage(currentOffset, toEmit);
            
            // Connecting to the publisher begins the stream
            output.setSource(data.autoConnect());
            
            // Lets presume a result with no items indicates the end.
            output.setState(data.count().filter(count -> count != 0).map(count -> currentOffset + count + 1));
        });
    }
    
    stream().take(111); // Will request 100 and then 11 items
    
Paginated by token:

    class PageWithData {
        String nextToken;
        List<Data> items;
    }

    // null for initial page
    private Flowable<PageWithData> getPage(String token);

    Flowable<Data> stream() {
        return FlowableBatched.create(Functions.justCallable(""), (token, requested, output) -> {
            ConnectableFlowable<PageWithData> data = getPage(token.isEmpty() ? null : token);
            
            // Connecting to the publisher begins the stream
            output.setSource(data.autoConnect().flatMapIterable(page -> page.items));
            
            // Lets presume a result with no items indicates the end.
            output.setState(
                    data.lastElement()
                        .map(page -> page.nextToken) // Extract token
                        .filter(token -> !token.isEmpty()) // Lack of a next token means end of stream
            ); 
        });
    }
    
