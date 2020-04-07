package com.mera.education.grpc.number.server;

import com.mera.education.grpc.proto.greet.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GreetServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    private static Logger logger = LoggerFactory.getLogger(GreetServiceImpl.class);

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        logger.debug("*** Unary implementation on server side ***");
        Greeting greeting = request.getGreeting();
//        logger.debug("Request has been received on server side: firstName - {}, lastName - {}", greeting.getFirstName(), greeting.getLastName());
        logger.debug("Request has been received on server side: number", greeting.getNumber());

        int number = greeting.getNumber();
        double sqrt = (double) Math.round(Math.sqrt(number) * 100) / 100;
        String result = "Hello my sqrt of number  " + number + " is " + sqrt;

        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();

        //send the response
        responseObserver.onNext(response);

        //complete RPC call
        responseObserver.onCompleted();
    }

    private String calculateStandardDeviation(int[] numbers) {
        int sum = 0;
        int max = 0;
        int min = numbers[0];
        double sd = 0;
        for (int item : numbers) {
            sum = sum + item;
        }
        double average = sum / numbers.length;
        System.out.println("Average value is : " + average);
        for (int value : numbers) {
            if (value > max) {
                max = value;
            }
        }
        System.out.println("max number is : " + max);
        for (int number : numbers) {
            if (number < min) {
                min = number;
            }
        }
        System.out.println("min number is : " + min);
        for (int number : numbers) {
            sd += ((number - average) * (number - average)) / (numbers.length - 1);
        }
        double standardDeviation = Math.sqrt(sd);
        System.out.println("The standard deviation is : " + standardDeviation);
        return String.valueOf(standardDeviation);
    }

    private String numberToFactors(int number) {
        List<Integer> factorsList = new ArrayList<>();
        for (int i = 1; i <= number; ++i) {
            if (number % i == 0) {
                factorsList.add(i);
            }
        }
        return Arrays.toString(factorsList.toArray());
    }

    private String getMaxValue(List<Integer> numbers) {
        int maxValue = numbers.get(0);
        for (int i = 1; i < numbers.size(); i++) {
            if (numbers.get(i) > maxValue) {
                maxValue = numbers.get(i);
            }
        }
        return String.valueOf(maxValue);
    }

    @Override
    public void greetmanyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        logger.debug("*** Server streaming implementation on server side ***");
        Greeting greeting = request.getGreeting();
        int number = greeting.getNumber();

        try {
            for (int i = 0; i < 10; i++) {
                GreetManyTimesResponse response = GreetManyTimesResponse.newBuilder()
                        .setResult(numberToFactors(number))
                        .build();
                logger.debug("send response {} of 10", i + 1);
                responseObserver.onNext(response);
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.debug("all messages have been sent");
            responseObserver.onCompleted();
        }

    }

    @Override
    public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
        logger.debug("*** Client streaming implementation on server side ***");
        StreamObserver<LongGreetRequest> streamObserverofRequest = new StreamObserver<LongGreetRequest>() {

            String result = "";

            List<Integer> list = new ArrayList();

            @Override
            public void onNext(LongGreetRequest longGreetRequest) {
                //client sends a message
                int number = longGreetRequest.getGreeting().getNumber();
                logger.debug("Adding number " + number + " to list of " + Arrays.toString(list.toArray()));
                list.add(number);
            }

            @Override
            public void onError(Throwable throwable) {
                //client sends an error
            }

            @Override
            public void onCompleted() {
//                client is done, this is when we want to return a response (responseObserver)

                int[] array = list.stream()
                        .mapToInt(Integer::intValue)
                        .toArray();

                responseObserver.onNext(LongGreetResponse.newBuilder()
                        .setResult(calculateStandardDeviation(array))
                        .build());
                logger.debug("Send result: - {}", result);
                responseObserver.onCompleted();
            }
        };

        return streamObserverofRequest;
    }


    @Override
    public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {
        logger.debug("*** Bi directional streaming implementation on server side ***");

        StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
            List<Integer> integers = new ArrayList<>();

            @Override
            public void onNext(GreetEveryoneRequest value) {
                //client sends a message
                int number = value.getGreeting().getNumber();
                integers.add(number);
                GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
                        .setResult(getMaxValue(integers))
                        .build();

                //send message for each request
                responseObserver.onNext(greetEveryoneResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                //nothing
            }

            @Override
            public void onCompleted() {
                logger.debug("close bi directional streaming");
                responseObserver.onCompleted();
            }
        };

        return requestObserver;
    }
}
