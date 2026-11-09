package example.orders;

import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface OrderActivities {
  void validateOrder(String orderId);

  void reserveInventory(String orderId);

  /** Fails transiently ~15% of attempts and declines (non-retryable) ~4% of orders. */
  String chargePayment(String orderId);

  String scheduleShipment(String orderId);

  void notifyCustomer(String orderId, String message);
}
