import pytest
from unittest.mock import MagicMock, patch
from src.ingestion.connectors.youtube_client import YoutubeAPIClient, MaxRetriesError
from pathlib import Path


@pytest.fixture
def youtube_client():
    # Fixture to create an instance of YoutubeAPIClient
    client = YoutubeAPIClient()
    return client


@pytest.fixture
def mock_api_response():
    # A mock response for YouTube API requests
    return {
        "items": [
            {
                "snippet": {
                    "title": "Sample Video",
                    "description": "Sample Description",
                    "channelTitle": "Sample Channel",
                    "publishedAt": "2025-04-01T00:00:00Z",
                    "thumbnails": {
                        "high": {
                            "url": "http://sample.url/thumbnail.jpg"
                        }
                    }
                },
                "id": {
                    "videoId": "123456"
                }
            }
        ],
        "nextPageToken": None
    }


def test_authenticate(youtube_client):
    # Test the authentication process
    with patch.object(youtube_client, 'authenticate') as mock_authenticate:
        youtube_client.authenticate()
        mock_authenticate.assert_called_once()


@patch("requests.get")
def test_fetch_success(mock_get, youtube_client, mock_api_response):
    # Test fetching data from the API
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_api_response

    response = youtube_client.fetch("search", {"q": "Machine Learning"})
    assert response == mock_api_response
    mock_get.assert_called_once()


@patch("requests.get")
def test_fetch_failure(mock_get, youtube_client):
    # Test fetch failure after retries
    mock_get.return_value.status_code = 500  # Simulate server error

    with pytest.raises(MaxRetriesError):
        youtube_client.fetch("search", {"q": "Machine Learning"})


@patch("src.ingestion.connectors.youtube_client.YoutubeAPIClient.retrieve_videos_basic_data")
def test_retrieve_videos_basic_data(mock_retrieve_videos, youtube_client):
    # Test the retrieval of basic video data
    mock_retrieve_videos.return_value = (
        [{"videoId": "123456", "title": "Sample Video", "url": "http://example.com"}],
        None
    )

    result, _ = youtube_client.retrieve_videos_basic_data("Machine Learning", limit=1)
    assert len(result) == 1
    assert result[0]["videoId"] == "123456"
    assert result[0]["title"] == "Sample Video"


@patch("src.ingestion.connectors.youtube_client.YoutubeAPIClient.retrieve_video_statistics")
def test_retrieve_video_statistics(mock_retrieve_stats, youtube_client):
    # Test retrieval of video statistics
    mock_retrieve_stats.return_value = {
        "123456": {
            "viewCount": 1000,
            "likeCount": 100,
            "commentCount": 50,
            "duration": "PT10M",
            "definition": "hd"
        }
    }

    stats = youtube_client.retrieve_video_statistics(["123456"])
    assert stats["123456"]["viewCount"] == 1000
    assert stats["123456"]["likeCount"] == 100
    assert stats["123456"]["duration"] == "PT10M"


@patch("src.ingestion.connectors.youtube_client.YoutubeAPIClient.download_image")
def test_extract_video_thumbnails(mock_download_image, youtube_client):
    # Test extracting video thumbnails
    mock_download_image.return_value = None  # Mock the download function

    videos = [{"videoId": "123456", "thumbnail": "http://sample.url/thumbnail.jpg"}]
    thumbnails = youtube_client.extract_video_thumbnails(videos, output_folder="test_folder")

    mock_download_image.assert_called_once_with("http://sample.url/thumbnail.jpg", Path("test_folder/123456.jpg"))


@patch("src.ingestion.connectors.youtube_client.YoutubeAPIClient.extract_audio")
def test_extract_audio_from_videos(mock_extract_audio, youtube_client):
    # Test extracting audio from videos
    mock_extract_audio.return_value = "test_path/audio.mp3"

    videos = [{"videoId": "123456", "title": "Sample Video"}]
    result = youtube_client.extract_audio_from_videos(videos, output_folder="test_folder")

    assert result[0]["audio_path"] == "test_path/audio.mp3"
    mock_extract_audio.assert_called_once_with("123456", Path("test_folder"))


@patch("src.ingestion.connectors.youtube_client.YoutubeAPIClient.extract_video")
def test_extract_video_from_videos(mock_extract_video, youtube_client):
    # Test extracting video from videos
    mock_extract_video.return_value = "test_path/video.mp4"

    videos = [{"videoId": "123456", "title": "Sample Video"}]
    result = youtube_client.extract_video_from_videos(videos, output_folder="test_folder")

    assert result[0]["video_path"] == "test_path/video.mp4"
    mock_extract_video.assert_called_once_with("123456", Path("test_folder"))


# Add more tests as needed for other methods


# Run the tests using pytest
if __name__ == "__main__":
    pytest.main()
