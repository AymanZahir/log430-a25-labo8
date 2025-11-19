"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import modify_order
from db import get_redis_conn

class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        payment_id = event_data.get("payment_id")
        payment_link = event_data.get("payment_link")
        if not payment_link and payment_id:
            payment_link = f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            event_data["payment_link"] = payment_link

        try:
            # Mettre à jour la commande avec le lien/ID de paiement et synchroniser Redis.
            modify_order(
                event_data["order_id"],
                is_paid=True,
                payment_id=payment_id,
                payment_link=payment_link
            )
            redis_client = get_redis_conn()
            order_key = f"order:{event_data['order_id']}"
            cached_order = redis_client.hgetall(order_key)
            cached_order.update({
                "payment_link": payment_link or "no-link",
                "is_paid": "True"
            })
            redis_client.hset(order_key, mapping=cached_order)
            # Si l'operation a réussi, déclenchez SagaCompleted.
            event_data['event'] = "SagaCompleted"
            self.logger.debug(f"payment_link={event_data['payment_link']}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)

        except Exception as e:
            # Si l'operation a échoué, déclenchez l'événement adéquat.
            event_data['error'] = str(e)
            event_data['event'] = "PaymentCreationFailed"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)

