package io.confluent.examples;

/**
 * Demo value type produced to and consumed from Kafka.
 *
 * The KafkaJsonSchemaSerializer derives a JSON Schema from this class's
 * getters at produce time, so a plain POJO with a no-arg constructor and
 * standard getters/setters is all that is required. The no-arg constructor
 * and setters are what let the KafkaJsonSchemaDeserializer rebuild the object
 * on the consumer side via Jackson.
 *
 * To model your own data, rename this class and change the fields/getters —
 * nothing else in the producer or consumer needs to know the field names.
 */
public class Order {
  private String orderId;
  private String product;
  private int quantity;
  private double price;

  // Required by Jackson for deserialization on the consumer side.
  public Order() {}

  public Order(String orderId, String product, int quantity, double price) {
    this.orderId = orderId;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
  }

  public String getOrderId() { return orderId; }
  public void setOrderId(String orderId) { this.orderId = orderId; }

  public String getProduct() { return product; }
  public void setProduct(String product) { this.product = product; }

  public int getQuantity() { return quantity; }
  public void setQuantity(int quantity) { this.quantity = quantity; }

  public double getPrice() { return price; }
  public void setPrice(double price) { this.price = price; }

  @Override
  public String toString() {
    return "Order{orderId='" + orderId + "', product='" + product
        + "', quantity=" + quantity + ", price=" + price + '}';
  }
}
