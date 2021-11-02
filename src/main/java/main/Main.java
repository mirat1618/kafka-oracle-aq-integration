package main;

import kafka.KafkaManager;

public class Main {
    public static void main(String[] args) {
        KafkaManager km1 = new KafkaManager();
        KafkaManager km2 = new KafkaManager();

        Thread listener = new Thread(new Runnable() {
            @Override
            public void run(){
                km1.startListening("to-be-enqueued");
            }
        });

        Thread streamer = new Thread(new Runnable() {
            @Override
            public void run(){
                km2.startStreaming("dequeued");
            }
        });

        listener.start();
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        streamer.start();
    }
}
