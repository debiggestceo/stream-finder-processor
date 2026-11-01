import unittest

from app import app, detect_platform, format_time, validate_video_url


class StreamFinderUtilityTests(unittest.TestCase):
    def test_format_time(self):
        self.assertEqual(format_time(0), "00:00:00")
        self.assertEqual(format_time(61), "00:01:01")
        self.assertEqual(format_time(3661), "01:01:01")

    def test_supported_platforms(self):
        self.assertEqual(detect_platform("https://kick.com/example"), "Kick")
        self.assertEqual(detect_platform("https://www.twitch.tv/example"), "Twitch")
        self.assertEqual(detect_platform("https://youtu.be/abcdefghijk"), "YouTube")

    def test_accepts_supported_video_urls(self):
        for url in (
            "https://kick.com/example",
            "https://www.kick.com/example",
            "https://twitch.tv/example",
            "https://www.twitch.tv/example",
            "https://youtube.com/watch?v=abcdefghijk",
            "https://www.youtube.com/watch?v=abcdefghijk",
            "https://youtu.be/abcdefghijk",
        ):
            self.assertEqual(validate_video_url(url), url)

    def test_rejects_unsupported_or_lookalike_hosts(self):
        for url in (
            "https://example.com/video",
            "https://kick.com.example.org/video",
            "https://notkick.com/video",
            "https://youtube.com.example.org/video",
            "ftp://youtube.com/video",
            "not a url",
        ):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    validate_video_url(url)

    def test_health_endpoint_shape(self):
        response = app.test_client().get("/")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "Stream Finder Processor is online")
        self.assertIn("platforms", payload)
        self.assertIn("vision_enabled", payload)


if __name__ == "__main__":
    unittest.main()
