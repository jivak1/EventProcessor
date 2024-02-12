public class Main {
    public static void main(String[] args) {

        EventProcessor processor = new EventProcessor(3, 100, 1000); // maxAttempts, initialDelay, maxDelay
        Thread processorThread = new Thread(processor);
        processorThread.start();

        EventProducer producer = new EventProducer(new EventListenerImpl(processor));
        producer.start();

        // simulate program running
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        processor.stop();

    }
}

public interface EventListener {
    void onEventReceived(Event event);
}


public class EventProducer {
    private EventListener eventListener;

    EventProducer(EventListener eventListener) {
        this.eventListener = eventListener;
    }

    void start() {
        // this is an example very basic implementation for illustration purposes;
        // in production, it will have a much more complex implementation reading from an external system
        eventListener.onEventReceived(new Event(1, "log", "line 1"));
        eventListener.onEventReceived(new Event(2, "lala", "line 2"));
    }
}


public class EventListenerImpl implements EventListener {
    private EventProcessor eventProcessor ;

    EventListenerImpl(EventProcessor eventProcessor){
        this.eventProcessor = eventProcessor ;

    }
    @Override
    public void onEventReceived(Event event) {
        eventProcessor.addEvent(event);
        System.out.println("Event received: " + event.getData());
    }
}


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventProcessor implements Runnable{

    private BlockingQueue<Event> eventQueue;
    private ExecutorService executorService;
    private int maxAttempts;
    private long initialDelay;
    private long maxDelay;

    public EventProcessor(int maxAttempts, long initialDelay, long maxDelay) {
        this.eventQueue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(1);
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    public void addEvent(Event event) {
        eventQueue.offer(event);
    }

    @Override
    public void run(){
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = eventQueue.take();
                executorService.submit(() -> processEvent(event));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        executorService.shutdown();
    }

    private void processEvent(Event event) {
        int attempts = 0;
        long delay = initialDelay;
        boolean processed = false;

        while (!processed && attempts < maxAttempts) {
            try {
                // complete or fail based on event type
                switch(event.getType()){
                    case "log":
                        System.out.println("Processing event: " + event.getData());

                        //success
                        processed = true;

                        System.out.println("Event processed successfully");


                        break ;
                    default:
                        System.out.println("Unknown event type: " + event.getType());

                        //failure
                        throw new RuntimeException("Failed to process event");
                }
            } catch (RuntimeException e) {
                System.out.println("Failed to process event, retrying in " + delay + "ms");

                attempts++;

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }

                //incrementing delay strategy
                if(!(delay > maxDelay)){
                    delay *= 2 ;
                }
            }
            if (!processed) {
                System.out.println("Failed to process event after " + attempts + " attempts: " + event.getId());
            }
        }
    }

    public void printEventQueue() {
        System.out.println("Contents of the event queue:");
        Event[] events = eventQueue.toArray(new Event[eventQueue.size()]);
        for (Event event : events) {
            System.out.println(event.getId() + ": " + event.getType() + " - " + event.getData());
        }
        if(events.length == 0){
            System.out.println("No elements left in the queue") ;
        }
    }

    public void stop() {
        executorService.shutdownNow();

        Thread.currentThread().interrupt();
    }

}

public class Event {
    private int id;
    private String type;
    private Object data;

    int getId() {
        return id;
    }

    String getType() {
        return type;
    }

    Object getData() {
        return data;
    }

    Event(int id, String type, Object data) {
        this.id = id;
        this.type = type;
        this.data = data;
    }
}
