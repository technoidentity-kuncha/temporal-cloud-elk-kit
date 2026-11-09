package example.orders;

import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.util.concurrent.ThreadLocalRandom;

/** Simulated side effects with realistic latency and failure, so metrics have shape. */
public class OrderActivitiesImpl implements OrderActivities {

  private static void work(int minMs, int maxMs) {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void validateOrder(String orderId) {
    work(20, 120);
  }

  @Override
  public void reserveInventory(String orderId) {
    work(50, 400);
  }

  @Override
  public String chargePayment(String orderId) {
    work(100, 900);
    double r = ThreadLocalRandom.current().nextDouble();
    if (r < 0.04 && Activity.getExecutionContext().getInfo().getAttempt() == 1) {
      throw ApplicationFailure.newNonRetryableFailure("card declined", "PaymentDeclined");
    }
    if (r < 0.19) {
      throw ApplicationFailure.newFailure("payment gateway timeout", "GatewayTimeout");
    }
    return "txn-" + orderId;
  }

  @Override
  public String scheduleShipment(String orderId) {
    work(100, 600);
    return "ship-" + orderId;
  }

  @Override
  public void notifyCustomer(String orderId, String message) {
    work(10, 80);
  }
}
