import json
import pytest
from pydantic import ValidationError
from gateway.config import Provider, Settings
from gateway.schemas import ChatRequest


def test_missing_real_key_fails_without_mock_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MISSING_TEST_UPSTREAM", raising=False)
    config = tmp_path / "routing.json"
    config.write_text(json.dumps({"providers": [{"name": "real", "base_url": "https://example.org/v1", "model": "test",
                      "api_key_env": "MISSING_TEST_UPSTREAM", "capabilities": ["text"]}], "routes": {"test": ["real"]}}))
    with pytest.raises(ValueError, match="credential"):
        Settings(_env_file=None, mode="real", api_key="private-test-key", config=config).load_routing()


@pytest.mark.parametrize("url", ["http://evil.example/v1", "https://key@example.org", "https://example.org?key=secret", "http://localhost.evil.org"])
def test_unsafe_upstream_config_rejected(url):
    with pytest.raises(ValidationError):
        Provider(name="test", base_url=url, model="test", api_key_env="TEST", capabilities={"text"})


@pytest.mark.parametrize("extra", [{"n": 2}, {"stream_options": {}}, {"response_format": {"type": "json_schema"}},
                                 {"messages": [{"role": "user", "content": [{"type": "image_url"}]}]}])
def test_unsupported_features_are_explicit(extra):
    with pytest.raises(ValidationError):
        ChatRequest.model_validate({"model": "test", "messages": [{"role": "user", "content": "hello"}], **extra})


def test_dotenv_upstream_credentials_are_server_owned(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("UPSTREAM_TEST_KEY", raising=False)
    (tmp_path / ".env").write_text("UPSTREAM_TEST_KEY=secret-test\n")
    config = tmp_path / "routing.json"
    config.write_text(json.dumps({"providers": [{"name": "real", "base_url": "https://example.org/v1", "model": "test",
                      "api_key_env": "UPSTREAM_TEST_KEY", "capabilities": ["text"]}], "routes": {"test": ["real"]}}))
    settings = Settings(_env_file=None, mode="real", api_key="client-test", config=config)
    provider = settings.load_routing().providers[0]
    assert provider.api_key == "secret-test"
    assert "secret-test" not in repr(provider)
    assert "api_key" not in provider.model_dump()
