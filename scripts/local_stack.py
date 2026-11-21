"""Reusable explicit mock stack; ``with local_stack() as stack`` owns all processes."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass

import httpx

ROOT = Path(__file__).resolve().parents[1]
AUTH = {"Authorization": "Bearer demo-client-key"}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def server(module, port, env, logfile):
    with logfile.open("w", encoding="utf-8") as log:
        process = subprocess.Popen([sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1",
                                    "--port", str(port), "--workers", "1", "--no-access-log"],
                                   cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            with httpx.Client(trust_env=False, timeout=.3) as client:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError(f"server exited: {logfile.read_text(encoding='utf-8')}")
                    try:
                        if client.get(f"http://127.0.0.1:{port}/health").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(.03)
                else:
                    raise RuntimeError(f"server not ready: {logfile}")
            yield process
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@dataclass
class Stack:
    url: str
    mock_url: str
    client: httpx.Client
    mock: httpx.Client

    def control(self, **kwargs):
        response = self.mock.post("/control", json=kwargs)
        response.raise_for_status()
        return response.json()

    def state(self):
        response = self.client.get("/admin/state")
        response.raise_for_status()
        return response.json()

    def wait_idle(self, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.mock.get("/stats").json()
            state = self.state()
            providers = state.get("providers", {})
            if isinstance(providers, list):
                providers = {str(i): provider for i, provider in enumerate(providers)}
            gates = [provider.get("gate", provider) for provider in providers.values()]
            if value["active"] == 0 and all(gate.get("active", 0) == gate.get("waiting", 0) == 0 for gate in gates):
                return value, state
            time.sleep(.03)
        raise AssertionError(f"resources not released: mock={value}, gateway={state}")


@contextmanager
def local_stack(*, provider_overrides=None, env_overrides=None, artifact_dir=None):
    with tempfile.TemporaryDirectory(prefix="llm-gateway-") as temporary:
        tmp_path = Path(artifact_dir) if artifact_dir else Path(temporary)
        tmp_path.mkdir(parents=True, exist_ok=True)
        mock_port, gateway_port = free_port(), free_port()
        configuration = json.loads((ROOT / "configs/mock.json").read_text(encoding="utf-8"))
        for provider in configuration["providers"]:
            provider["base_url"] = f"http://127.0.0.1:{mock_port}/v1"
            provider.update((provider_overrides or {}).get(provider["name"], {}))
        config_path = tmp_path / f"routing-{gateway_port}.json"
        config_path.write_text(json.dumps(configuration), encoding="utf-8")
        env = os.environ.copy()
        for name in list(env):
            if name.startswith("GATEWAY_"):
                env.pop(name)
        env.update({"GATEWAY_CONFIG": str(config_path), "GATEWAY_MODE": "mock", "GATEWAY_API_KEY": "demo-client-key",
                    "GATEWAY_RATE_BACKEND": "memory", "GATEWAY_DEADLINE_SECONDS": "3", "GATEWAY_QUEUE_TIMEOUT_SECONDS": "1",
                    "GATEWAY_MAX_ATTEMPTS": "3", "GATEWAY_RETRY_DELAY_SECONDS": ".05",
                    "GATEWAY_BREAKER_MIN_SAMPLES": "5", "GATEWAY_BREAKER_FAILURE_RATIO": ".5",
                    "GATEWAY_BREAKER_WINDOW_SECONDS": "30", "GATEWAY_BREAKER_COOLDOWN_SECONDS": "5",
                    "MOCK_PRIMARY_KEY": "mock-upstream-key", "MOCK_SECONDARY_KEY": "mock-upstream-key"})
        env.update(env_overrides or {})
        with server("gateway.mock_provider:app", mock_port, env, tmp_path / f"mock-{mock_port}.log"):
            with server("gateway.api:app", gateway_port, env, tmp_path / f"gateway-{gateway_port}.log"):
                with httpx.Client(base_url=f"http://127.0.0.1:{gateway_port}", headers=AUTH, trust_env=False, timeout=5) as client:
                    with httpx.Client(base_url=f"http://127.0.0.1:{mock_port}", trust_env=False, timeout=5) as mock:
                        yield Stack(str(client.base_url).rstrip("/"), str(mock.base_url).rstrip("/"), client, mock)
