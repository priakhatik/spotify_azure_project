"""Offline safety checks for the Azure Data Factory alerting configuration."""

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / "pipeline" / "spotify_incremental_loop.json"


class AlertConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = PIPELINE_PATH.read_text(encoding="utf-8")
        cls.pipeline = json.loads(cls.raw)
        cls.alert = next(
            activity for activity in cls.pipeline["properties"]["activities"]
            if activity["name"] == "Alerts"
        )

    def test_signed_webhook_not_committed(self):
        self.assertNotIn("sig=", self.raw.lower())
        self.assertNotIn("logic.azure.com/", self.raw.lower())

    def test_webhook_url_comes_from_secure_runtime_parameter(self):
        parameters = self.pipeline["properties"]["parameters"]
        self.assertEqual(parameters["alert_webhook_url"]["type"], "SecureString")
        self.assertNotIn("defaultValue", parameters["alert_webhook_url"])
        self.assertEqual(
            self.alert["typeProperties"]["url"],
            {"value": "@pipeline().parameters.alert_webhook_url", "type": "Expression"},
        )

    def test_web_activity_hides_secret_bearing_inputs_and_outputs(self):
        self.assertTrue(self.alert["policy"]["secureInput"])
        self.assertTrue(self.alert["policy"]["secureOutput"])


if __name__ == "__main__":
    unittest.main()
