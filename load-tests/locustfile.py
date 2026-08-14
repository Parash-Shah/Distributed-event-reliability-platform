from uuid import uuid4

from locust import HttpUser, between, task


class EventUser(HttpUser):
    wait_time = between(0.01, 0.05)

    @task(20)
    def submit_normal_event(self):
        event_id = str(uuid4())
        self.client.post(
            "/api/v1/events",
            headers={"Idempotency-Key": event_id},
            json={"event_type": "ORDER_CREATED", "payload": {"order_id": event_id}},
            name="POST /events [normal]",
        )

    @task(1)
    def submit_duplicate_pair(self):
        event_id = str(uuid4())
        body = {"event_type": "ORDER_CREATED", "payload": {"order_id": event_id}}
        headers = {"Idempotency-Key": event_id}
        self.client.post("/api/v1/events", headers=headers, json=body, name="POST /events [duplicate]")
        self.client.post("/api/v1/events", headers=headers, json=body, name="POST /events [duplicate]")
