import pytest
import os
from unittest.mock import patch, mock_open
from src.utils.config import ConfigManager  # Adjust the import as per your module

# This mock simulates the presence of a configuration file
@pytest.fixture
def mock_config():
    config_data = """
    apis:
      youtube:
        base_url: "https://youtube.googleapis.com"
        endpoints:
          search: "/v3/search"
          videos: "/v3/videos"
        auth:
          type: "api_key"
          api_key_env: "YOUTUBE_API_KEY"
      twitter:
        base_url: "https://api.twitter.com"
        endpoints:
          tweets: "/2/tweets"
        auth:
          type: "username_password"
          username_env: "TWITTER_USERNAME"
          password_env: "TWITTER_PASSWORD"
    """
    # Mock the open function to simulate file reading
    with patch("builtins.open", mock_open(read_data=config_data)), patch("os.path.exists", return_value=True):
        yield ConfigManager(config_path="mock_config_path")  # Using mock config path

# Test loading configuration
def test_load_config(mock_config):
    assert mock_config.get_api_base_url("youtube") == "https://youtube.googleapis.com"
    assert mock_config.get_api_endpoints("youtube") == {"search": "/v3/search", "videos": "/v3/videos"}
    assert mock_config.get_api_base_url("twitter") == "https://api.twitter.com"

# Test missing file
@patch("os.path.exists", return_value=False)
def test_load_config_file_not_found(mock_exists):
    with pytest.raises(FileNotFoundError):
        ConfigManager(config_path="non_existing_file.yaml")

# Test getting API credentials for API key
@patch.dict(os.environ, {"YOUTUBE_API_KEY": "fake_api_key"})
def test_get_api_credentials_api_key(mock_config):
    credentials = mock_config.get_api_credentials("youtube")
    assert credentials == {"api_key": "fake_api_key"}

# Test missing environment variable for API key
@patch.dict(os.environ, {}, clear=True)
def test_get_api_credentials_api_key_missing(mock_config):
    with pytest.raises(ValueError) as exc:
        mock_config.get_api_credentials("youtube")
    assert "Missing API key for youtube" in str(exc.value)

# Test getting credentials for username/password authentication
@patch.dict(os.environ, {"TWITTER_USERNAME": "user123", "TWITTER_PASSWORD": "pass123"})
def test_get_api_credentials_username_password(mock_config):
    credentials = mock_config.get_api_credentials("twitter")
    assert credentials == {"username": "user123", "password": "pass123"}

# Test missing environment variables for username/password authentication
@patch.dict(os.environ, {}, clear=True)
def test_get_api_credentials_username_password_missing(mock_config):
    with pytest.raises(ValueError) as exc:
        mock_config.get_api_credentials("twitter")
    assert "Missing credentials for twitter" in str(exc.value)

# Test retrieving config with dynamic keys
def test_get_config(mock_config):
    assert mock_config.get_config("apis", "youtube", "base_url") == "https://youtube.googleapis.com"
    assert mock_config.get_config("apis", "youtube", "auth", "type") == "api_key"
    assert mock_config.get_config("apis", "twitter", "auth", "username_env") == "TWITTER_USERNAME"
    assert mock_config.get_config("non_existing_key", default="default_value") == "default_value"

# Test unknown authentication type
def test_get_api_credentials_unknown_auth_type(mock_config):
    mock_config.config["apis"]["unknown_api"] = {
        "base_url": "https://example.com",
        "endpoints": {"example": "/v1/example"},
        "auth": {"type": "unknown_auth_type"},
    }
    with pytest.raises(ValueError) as exc:
        mock_config.get_api_credentials("unknown_api")
    assert "Unknown authentication type" in str(exc.value)
