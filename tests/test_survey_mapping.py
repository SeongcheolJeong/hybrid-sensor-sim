from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from hybrid_sensor_sim.io.survey_mapping import generate_survey_from_scenario


class SurveyMappingTests(unittest.TestCase):
    def test_generate_survey_from_scenario_with_waypoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                """
{
  "name": "urban-test",
  "objects": [
    {
      "id": "ego",
      "type": "vehicle",
      "pose": [1.0, 2.0, 3.0],
      "waypoints": [
        [4.0, 5.0, 6.0],
        {"x": 7.0, "y": 8.0, "z": 9.0}
      ]
    }
  ]
}
""".strip(),
                encoding="utf-8",
            )
            output_dir = root / "generated"
            out = generate_survey_from_scenario(
                scenario_path=scenario,
                output_dir=output_dir,
                options={},
            )
            self.assertTrue(out.exists())
            tree = ET.parse(out)
            root_node = tree.getroot()
            self.assertEqual(root_node.tag, "document")
            survey = root_node.find("survey")
            self.assertIsNotNone(survey)
            assert survey is not None
            self.assertEqual(survey.attrib.get("name"), "urban-test")
            legs = survey.findall("leg")
            self.assertEqual(len(legs), 3)

    def test_generate_survey_from_invalid_json_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text("{invalid-json", encoding="utf-8")
            with self.assertRaises(ValueError):
                generate_survey_from_scenario(
                    scenario_path=scenario,
                    output_dir=root / "generated",
                    options={},
                )


if __name__ == "__main__":
    unittest.main()
