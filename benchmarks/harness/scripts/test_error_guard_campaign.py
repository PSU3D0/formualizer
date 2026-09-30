# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""uv run benchmarks/harness/scripts/test_error_guard_campaign.py"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("error-guard-campaign.py")
spec = importlib.util.spec_from_file_location("campaign", SCRIPT)
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


class CampaignTest(unittest.TestCase):
    def test_interleave(self):
        order = list(campaign.schedule(list("ABCD"), 4))
        self.assertEqual(len(order), 16)
        for arm in "ABCD":
            self.assertEqual(sum(a == arm for _, a in order), 4)
        self.assertNotEqual([a for r, a in order if r == 0], [a for r, a in order if r == 1])

    def test_wrong_answers_are_not_comparison_eligible(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            arms = []
            for name, correct in [("A", False), ("D", True), ("E", True)]:
                binary = root / name
                binary.write_text(f"#!{sys.executable}\n" + '''import json, pathlib, sys
out = pathlib.Path(sys.argv[sys.argv.index('--output-dir') + 1])
out.mkdir()
correct = ''' + repr(correct) + '''
notes = ''' + repr(["expected invariant failure: wrong cell", "expected_failure_reason: known defect"] if name == "E" else []) + '''
(out / 's089-off.json').write_text(json.dumps({'final_invariants_passed': correct, 'notes': notes}))
(out / 'summary.csv').write_text('scenario_id,phase,wall_ms\\ns089,phase_first_eval,12\\ns089,phase_recalc_0,2\\n')
sys.exit(0 if correct else 1)
''')
                binary.chmod(0o700)
                arms.append({"id": name, "commit": "a" * 40, "build_notes": "mock",
                             "corpus": str(binary)})
            config = root / "config.json"
            config.write_text(json.dumps({"arms": arms, "repeats": 3,
                "cases": [{"id": "guards", "kind": "corpus", "include": "s089*"}]}))
            subprocess.run([sys.executable, str(SCRIPT), str(config), str(root / 'out')], check=True)
            summary = json.loads((root / 'out/summary.json').read_text())
            self.assertTrue(summary)
            for row in summary:
                self.assertEqual(row['comparison_eligible'], row['arm'] == 'D')
                if row['arm'] == 'A':
                    self.assertIsNone(row['median_ms'])
            provenance = json.loads((root / 'out/provenance.json').read_text())
            self.assertEqual(len(provenance['binary_sha256']), 3)
            self.assertEqual(len(list((root / 'out').glob('r*-*/run.json'))), 9)

    def test_program1_requires_trusted_digests_and_disables_counting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'program1'
            binary.write_text(f'#!{sys.executable}\n' + '''import json, sys
assert '--no-alloc-count' in sys.argv
print(json.dumps({'first_ok': True, 'value_edit_errors': 0, 'formula_edit_errors': 0,
'digest_first': 'initial', 'digest_end': 'edited', 'load_ms': 1,
'first_eval_ms': 2, 'value_recalc_p50_ms': 3}))
''')
            binary.chmod(0o700)
            xlsx = root / 'fixture.xlsx'
            xlsx.write_bytes(b'mock xlsx')
            config = root / 'config.json'
            config.write_text(json.dumps({'repeats': 3,
                'arms': [{'id': 'D', 'commit': 'd' * 40, 'build_notes': 'mock', 'program1': str(binary)}],
                'cases': [{'id': 'unverified', 'kind': 'program1', 'xlsx': str(xlsx)},
                          {'id': 'verified', 'kind': 'program1', 'xlsx': str(xlsx),
                           'expected_digests': {'digest_first': 'initial', 'digest_end': 'edited'}}]}))
            subprocess.run([sys.executable, str(SCRIPT), str(config), str(root / 'out')], check=True)
            summary = json.loads((root / 'out/summary.json').read_text())
            self.assertEqual(len(summary), 6)
            for row in summary:
                self.assertEqual(row['comparison_eligible'], row['case'] == 'verified')
                if row['case'] == 'unverified':
                    self.assertIsNone(row['median_ms'])


if __name__ == '__main__':
    unittest.main()
