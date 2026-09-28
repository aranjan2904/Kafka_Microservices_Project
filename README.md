"# Kafka_Microservices_Project" 


# Kafka Order Processing System

A small project where four Spring Boot services talk to each other only through Apache Kafka. You place an order, and the payment, delivery and notification steps happen on their own in the background.

I built it to learn how Kafka works: producers, consumers, consumer groups, partitions, offsets, retries, idempotency and dead letter topics.

## How it works

```
POST /orders -> Order Service -> [order-created] -> Payment Service
                                                        |
                              +-------------------------+
                              v                         v
                      [payment-success]          [payment-failed]
                              |                         |
                       Delivery Service                 |
                              |                         |
                      [delivery-created]                |
                              |                         |
                              +--> Notification Service <+
```

The Notification Service also listens to `order-created`, so the customer gets a message at every step.

If a payment fails, the flow stops there and the customer gets a failure notification. No delivery is created.

## Services

| Service | Port | What it does |
|---|---|---|
| order-service | 8081 | Takes the order, saves it, publishes `ORDER_CREATED` |
| payment-service | 8082 | Fake payment, publishes `PAYMENT_SUCCESS` or `PAYMENT_FAILED` |
| delivery-service | 8083 | Creates a delivery with a tracking number, publishes `DELIVERY_CREATED` |
| notification-service | 8084 | Prints a fake SMS/email for each event |

## Kafka setup

| Topic | Published by | Read by (consumer group) |
|---|---|---|
| order-created | order-service | payment-service-group, notification-service-group |
| payment-success | payment-service | delivery-service-group, notification-service-group |
| payment-failed | payment-service | notification-service-group |
| delivery-created | delivery-service | notification-service-group |
| payment-success.DLT | delivery-service | nobody, it's where bad messages end up |

Every topic has 3 partitions and the message key is the `orderId`, so all events of one order stay in order.

## Running it

You need Java 17+, Maven and Docker.

1. Start Kafka:
   ```
   docker compose up -d
   ```
2. Start each service in its own terminal:
   ```
   cd order-service
   mvn spring-boot:run
   ```
   Do the same for `payment-service`, `delivery-service` and `notification-service`.
3. Create an order:
   ```
   curl -X POST http://localhost:8081/orders \
     -H "Content-Type: application/json" \
     -d '{"customerId":101,"customerName":"Rahul","productId":501,"productName":"Laptop","quantity":1,"amount":75000,"deliveryAddress":"Bangalore"}'
   ```
   You should get back `{"orderId":1001,"status":"CREATED"}`. Now watch the logs of the other services.

There's also a Postman collection in the `postman/` folder with ready-made requests.

## Things I tried

- **Two groups, same event:** `order-created` is read by both payment and notification, each with its own group, so both get every message.
- **Partitions:** created a few orders and checked in Kafka which partition each one landed in.
- **Scaling:** started two payment-service instances in the same group. Kafka split the partitions between them.
- **Failure and retry:** stopped payment-service, created an order, started it again. The order was processed after the restart because the message was waiting in Kafka.
- **Duplicate message:** sent the same event twice. The `eventId` is checked before processing, so the payment happens only once.
- **Dead letter topic:** an unprocessable message is retried a few times and then moved to `payment-success.DLT`.

Screenshots of all of these are in the `screenshots/` folder.

## Tech used

Java, Spring Boot, Spring Kafka, Spring Data JPA, Apache Kafka, Docker, Postman

## Project structure

```
order-service/
payment-service/
delivery-service/
notification-service/
postman/
```
