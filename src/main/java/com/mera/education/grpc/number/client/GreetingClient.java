package com.mera.education.grpc.number.client;

import com.mera.education.grpc.proto.greet.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

    private static Logger logger = LoggerFactory.getLogger(GreetingClient.class);

    private String ipAddress = "localhost";
    private int port = 5051;

    public static void main(String[] args) {
        logger.debug("gRPC Client is started");
        GreetingClient main = new GreetingClient();
        main.run();
    }

    private void run() {
        logger.debug("Channel is created on {} with port: {}", ipAddress, port);
        //gRPC provides a channel construct which abstracts out the underlying details like connection, connection pooling, load balancing, etc.
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ipAddress, port)
                .usePlaintext()
                .build();

        //unary implementation
        doUnaryCall(channel, 49);

        //server streaming implementation
        doServerStreamingCall(channel, 125);

//        client streaming implementation
        int[] integers = {10, 12, 1, 2, 30, 44, 100};
        doClientStreamingCall(channel, integers);

        //bi directional streaming implementation
        doBiDiStreamingCall(channel, integers);

        logger.debug("Shutting down channel");
        channel.shutdown();
    }


    private void doUnaryCall(ManagedChannel channel, Integer number) {
        logger.debug("*** Unary implementation ***");
        //created a greet service client (blocking - sync)
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        //Unary
        //created a protocol buffer greeting message
        Greeting greeting = Greeting.newBuilder()
                .setNumber(number)
                .build();

        // the same for request
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting)
                .build();


        //call RPC call and get result
        GreetResponse greetResponse = greetClient.greet(greetRequest);

        logger.debug("Response has been received from server: - {}", greetResponse.getResult());
    }

    private void doServerStreamingCall(ManagedChannel channel, int number) {
        logger.debug("*** Server streaming implementation ***");
        //created a greet service client (blocking - sync)
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);
        //Server Streaming
        GreetManyTimesRequest greetManyTimesRequest = GreetManyTimesRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setNumber(number))
                .build();

        logger.debug("Send number ", number);
        greetClient.greetmanyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    logger.debug("Response has been received from server: - {}", greetManyTimesResponse.getResult());
                });
    }

    private void doClientStreamingCall(ManagedChannel channel, int[] numbers) {
        logger.debug("*** Client streaming implementation ***");
        //created a greet service client (async)
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<LongGreetRequest> requestObserver = asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse longGreetResponse) {
                //we get a response from the server, onNext will be called only once
                logger.debug("Received STD from the server: {}", longGreetResponse.getResult());
            }

            @Override
            public void onError(Throwable throwable) {
                //we get an error from the server
            }

            @Override
            public void onCompleted() {
                //the server is done sending us data
                //onCompleted will be called right after onNext()
                logger.debug("Server has completed sending us something");
                latch.countDown();
            }
        });

        for (int i = 0; i < numbers.length; i++) {
            logger.debug("Sending message #" + i);
            logger.debug("Number sent " + numbers[i]);
            requestObserver.onNext(LongGreetRequest.newBuilder()
                    .setGreeting(Greeting.newBuilder()
                            .setNumber(numbers[i])
                            .build())
                    .build());
        }

        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void doBiDiStreamingCall(ManagedChannel channel, int[] numbers) {
        logger.debug("*** Bi directional streaming implementation ***");
        //created a greet service client (async)
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse greetEveryoneResponse) {
                logger.debug("Max value from the server: {}", greetEveryoneResponse.getResult());
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.debug("Server is done sending data");
                latch.countDown();
            }
        });

        for (int number : numbers) {
            logger.debug("Sending: {}", number);
            requestObserver.onNext(GreetEveryoneRequest.newBuilder()
                    .setGreeting(Greeting.newBuilder()
                            .setNumber(number)
                            .build())
                    .build());

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
