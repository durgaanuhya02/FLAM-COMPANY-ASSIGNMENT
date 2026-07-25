import unittest

from queuectl.backoff import compute_delay


class TestBackoff(unittest.TestCase):
    def test_base2_matches_spec_example(self):
        # spec: base=2 -> 1st retry after 2s, then 4s, 8s
        self.assertEqual(compute_delay(1, 2), 2.0)
        self.assertEqual(compute_delay(2, 2), 4.0)
        self.assertEqual(compute_delay(3, 2), 8.0)

    def test_other_base(self):
        self.assertEqual(compute_delay(1, 3), 3.0)
        self.assertEqual(compute_delay(2, 3), 9.0)

    def test_zero_attempts_is_immediate(self):
        self.assertEqual(compute_delay(0, 2), 1.0)


if __name__ == "__main__":
    unittest.main()
