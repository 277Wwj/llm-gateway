"""Real HTTP AgentRAG -> gateway -> explicit mock, isolated runtime and no .env edits."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.local_stack import local_stack, free_port


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agentrag", type=Path, default=ROOT.parent / "agent-rag")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/agentrag-integration.json")
    args = parser.parse_args()
    agent = args.agentrag.resolve()
    python = agent / ".venv/Scripts/python.exe"
    if not python.exists():
        python = agent / ".venv/bin/python"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    log_dir = args.output.parent / "agentrag-logs"
    log_dir.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gateway-agentrag-") as runtime:
        with local_stack(artifact_dir=log_dir, env_overrides={"GATEWAY_DEADLINE_SECONDS": "15"}) as stack:
            port = free_port()
            env = os.environ.copy()
            env.update({"PYTHONPATH": str(agent), "PYTHONIOENCODING": "utf-8", "NO_PROXY": "*",
                        "AGENTRAG_PROVIDER": "chat_completions", "AGENTRAG_MODEL_BASE_URL": stack.url + "/v1",
                        "AGENTRAG_MODEL_NAME": "demo-chat", "AGENTRAG_MODEL_API_KEY": "demo-client-key",
                        "AGENTRAG_MODEL_TIMEOUT": "20", "AGENTRAG_DATA_DIR": runtime,
                        "AGENTRAG_DOCS_DIR": str(agent / "data/docs"), "AGENTRAG_RETRIEVAL_MODE": "lexical",
                        "AGENTRAG_EMBEDDING_PROVIDER": "hash", "AGENTRAG_MAX_STEPS": "12"})
            with (log_dir / "agentrag.log").open("w", encoding="utf-8") as log:
                process = subprocess.Popen([str(python), "-m", "uvicorn", "agent_rag.api:app", "--host", "127.0.0.1",
                                            "--port", str(port), "--workers", "1", "--no-access-log"],
                                           env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                try:
                    with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30, trust_env=False) as client:
                        until = time.monotonic() + 30
                        while time.monotonic() < until:
                            if process.poll() is not None:
                                raise RuntimeError("AgentRAG exited; inspect artifacts/agentrag-logs/agentrag.log")
                            try:
                                if client.get("/health").status_code == 200:
                                    break
                            except httpx.HTTPError:
                                pass
                            time.sleep(.1)
                        else:
                            raise RuntimeError("AgentRAG startup timeout")
                        def post(path, body):
                            response = client.post(path, json=body)
                            response.raise_for_status()
                            return response.json()
                        question = "Atlas 连接池耗尽时应如何排查？"
                        answer = post("/sessions", {"question": question, "follow_up": False})
                        assert answer["status"] == "complete" and answer["citations"] and answer["draft"] is None
                        draft = post("/sessions", {"question": question, "follow_up": True})
                        assert draft["status"] == "awaiting_confirmation" and draft["ticket"] is None
                        confirmation = {"version": draft["draft"]["version"], "confirmation_token": draft["draft"]["confirmation_token"]}
                        path = f"/sessions/{draft['session_id']}/confirm"
                        confirmed = post(path, confirmation)
                        repeated = post(path, confirmation)
                        assert confirmed["status"] == "complete" and confirmed["ticket"] == repeated["ticket"]
                        assert confirmed["ticket"]["simulated"]
                        stats, state = stack.wait_idle()
                        assert stats["requests"] == 2
                        # Do not persist random confirmation capabilities in evidence.
                        for value in (draft, confirmed, repeated):
                            if value.get("draft"):
                                value["draft"].pop("confirmation_token", None)
                        record = {"scope": "Explicit mock protocol integration, not real LLM quality", "agentrag_source": str(agent),
                                  "isolated_runtime": True, "user_env_edited": False, "answer": answer, "draft": draft,
                                  "confirmed": confirmed, "repeat_same_ticket": True, "mock_stats": stats, "gateway_state": state}
                        args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
                        print(json.dumps({"passed": True, "citations": len(answer["citations"]), "upstream_requests": stats["requests"],
                                          "repeat_same_ticket": True, "output": str(args.output)}, ensure_ascii=False))
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)


if __name__ == "__main__":
    main()
