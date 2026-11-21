from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        kw = {"registry": self.registry}
        self.requests = Counter("gateway_requests_total", "Authenticated validated logical requests", **kw)
        self.outcomes = Counter("gateway_request_outcomes_total", "Logical terminal outcomes", ["outcome"], **kw)
        self.attempts = Counter("gateway_upstream_attempts_total", "Actual upstream HTTP attempts", ["provider"], **kw)
        self.retries = Counter("gateway_retries_total", "Additional HTTP attempts", ["provider"], **kw)
        self.queue = Histogram("gateway_queue_seconds", "Provider admission wait", ["provider"], **kw)
        self.first_content = Histogram("gateway_first_content_seconds", "First nonempty content from logical request start", **kw)
        self.duration = Histogram("gateway_duration_seconds", "Logical total duration including queue and retries", ["outcome"], **kw)
        self.active = Gauge("gateway_provider_active", "Held concurrency slots", ["provider"], **kw)
        self.waiting = Gauge("gateway_provider_waiting", "Queued requests", ["provider"], **kw)
        self.circuit = Gauge("gateway_circuit_state", "0 closed, 1 open, 2 half-open", ["provider"], **kw)
