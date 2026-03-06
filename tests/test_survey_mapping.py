from __future__ import annotations

import json
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

    def test_generate_survey_from_explicit_helios_legs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                json.dumps(
                    {
                        "name": "explicit-legs",
                        "sensors": {
                            "lidar": {
                                "pulse_freq_hz": 200000,
                                "scan_freq_hz": 20,
                                "head_rotate_per_sec_deg": 3.0,
                            }
                        },
                        "helios": {
                            "scene": "data/scenes/base_scene.xml#base_scene",
                            "platform": "data/platforms.xml#vehicle_rig",
                            "scanner": "data/scanners_tls.xml#veh_lidar",
                            "legs": [
                                {
                                    "platform": {"x": 1.0, "y": 2.0, "z": 3.0},
                                    "scanner": {
                                        "template": "leg_template",
                                        "head_rotate_start_deg": -90.0,
                                    },
                                },
                                {"pose": [4.0, 5.0, 6.0]},
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "generated"
            out = generate_survey_from_scenario(
                scenario_path=scenario,
                output_dir=output_dir,
                options={
                    "survey_scene_ref": "data/scenes/override_scene.xml#override_scene",
                    "survey_scanner_settings_id": "global_set",
                },
            )
            self.assertTrue(out.exists())

            tree = ET.parse(out)
            root_node = tree.getroot()
            scanner_settings = root_node.find("scannerSettings")
            self.assertIsNotNone(scanner_settings)
            assert scanner_settings is not None
            self.assertEqual(scanner_settings.attrib.get("id"), "global_set")
            self.assertEqual(scanner_settings.attrib.get("pulseFreq_hz"), "200000")
            self.assertEqual(scanner_settings.attrib.get("scanFreq_hz"), "20")

            survey = root_node.find("survey")
            self.assertIsNotNone(survey)
            assert survey is not None
            self.assertEqual(survey.attrib.get("scene"), "data/scenes/override_scene.xml#override_scene")
            self.assertEqual(survey.attrib.get("platform"), "data/platforms.xml#vehicle_rig")
            self.assertEqual(survey.attrib.get("scanner"), "data/scanners_tls.xml#veh_lidar")

            legs = survey.findall("leg")
            self.assertEqual(len(legs), 2)
            first_platform = legs[0].find("platformSettings")
            self.assertIsNotNone(first_platform)
            assert first_platform is not None
            self.assertEqual(first_platform.attrib.get("x"), "1.000000")
            self.assertEqual(first_platform.attrib.get("y"), "2.000000")
            self.assertEqual(first_platform.attrib.get("z"), "3.000000")

            first_scanner = legs[0].find("scannerSettings")
            self.assertIsNotNone(first_scanner)
            assert first_scanner is not None
            self.assertEqual(first_scanner.attrib.get("template"), "leg_template")
            self.assertEqual(first_scanner.attrib.get("headRotatePerSec_deg"), "3.000000")
            self.assertEqual(first_scanner.attrib.get("headRotateStart_deg"), "-90.000000")
            self.assertEqual(first_scanner.attrib.get("headRotateStop_deg"), "180.000000")

            second_scanner = legs[1].find("scannerSettings")
            self.assertIsNotNone(second_scanner)
            assert second_scanner is not None
            self.assertEqual(second_scanner.attrib.get("template"), "global_set")
            self.assertEqual(second_scanner.attrib.get("headRotateStart_deg"), "-180.000000")

    def test_generate_survey_from_ego_trajectory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                json.dumps(
                    {
                        "name": "ego-trajectory-priority",
                        "ego_trajectory": [
                            {"position": {"x": 10.0, "y": 0.0, "z": 0.0}},
                            {"pose": [11.0, 1.0, 0.0]},
                        ],
                        "objects": [
                            {
                                "id": "ego",
                                "type": "vehicle",
                                "pose": [0.0, 0.0, 0.0],
                                "waypoints": [[1.0, 1.0, 1.0]],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            out = generate_survey_from_scenario(
                scenario_path=scenario,
                output_dir=root / "generated",
                options={},
            )
            tree = ET.parse(out)
            survey = tree.getroot().find("survey")
            self.assertIsNotNone(survey)
            assert survey is not None
            legs = survey.findall("leg")
            self.assertEqual(len(legs), 2)

            first_platform = legs[0].find("platformSettings")
            self.assertIsNotNone(first_platform)
            assert first_platform is not None
            self.assertEqual(first_platform.attrib.get("x"), "10.000000")
            self.assertEqual(first_platform.attrib.get("y"), "0.000000")

            second_platform = legs[1].find("platformSettings")
            self.assertIsNotNone(second_platform)
            assert second_platform is not None
            self.assertEqual(second_platform.attrib.get("x"), "11.000000")
            self.assertEqual(second_platform.attrib.get("y"), "1.000000")

    def test_generate_survey_force_global_leg_scanner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                json.dumps(
                    {
                        "name": "force-global-leg-scanner",
                        "helios_legs": [
                            {
                                "platformSettings": {"x": 0.0, "y": 0.0, "z": 0.0},
                                "scannerSettings": {"headRotateStart_deg": -30.0},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            out = generate_survey_from_scenario(
                scenario_path=scenario,
                output_dir=root / "generated",
                options={
                    "survey_force_global_leg_scanner": True,
                    "survey_head_rotate_start_deg": -120.0,
                },
            )
            tree = ET.parse(out)
            survey = tree.getroot().find("survey")
            self.assertIsNotNone(survey)
            assert survey is not None
            legs = survey.findall("leg")
            self.assertEqual(len(legs), 1)
            scanner = legs[0].find("scannerSettings")
            self.assertIsNotNone(scanner)
            assert scanner is not None
            self.assertEqual(scanner.attrib.get("headRotateStart_deg"), "-120.000000")


if __name__ == "__main__":
    unittest.main()
