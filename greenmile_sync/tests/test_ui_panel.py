import os
import sys
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class _DummyVar:
    def __init__(self, value=''):
        self.value = value

    def set(self, value):
        self.value = value

    def get(self):
        return self.value


class TestUiPanelIndicators(unittest.TestCase):

    def test_indicator_and_field_labels_are_operationally_explicit(self):
        from ui_panel import PANEL_FIELD_LABELS, PANEL_INDICATOR_LABELS

        self.assertEqual(PANEL_FIELD_LABELS['SNAPSHOT_REUSE_TTL_SECONDS'], 'TTL Cache Snapshots (s)')
        self.assertEqual(PANEL_INDICATOR_LABELS['pending_rows'], 'Linhas pendentes')
        self.assertEqual(PANEL_INDICATOR_LABELS['last_post_succeeded_at'], 'Ultimo POST OK')

    def test_update_indicators_from_result_updates_all_indicator_vars(self):
        from ui_panel import ConfigPanel

        panel = ConfigPanel.__new__(ConfigPanel)
        panel.indicator_vars = {
            'pending_rows': _DummyVar(),
            'moved_rows': _DummyVar(),
            'last_post_succeeded_at': _DummyVar(),
        }

        panel._update_indicators_from_result({
            'pending_rows': 12,
            'moved_rows': 5,
            'last_post_succeeded_at': '2026-04-08T16:30:00Z',
        })

        self.assertEqual(panel.indicator_vars['pending_rows'].get(), '12')
        self.assertEqual(panel.indicator_vars['moved_rows'].get(), '5')
        self.assertEqual(panel.indicator_vars['last_post_succeeded_at'].get(), '08/04/2026 13:30:00')

    def test_update_indicators_from_result_uses_dash_fallbacks(self):
        from ui_panel import ConfigPanel

        panel = ConfigPanel.__new__(ConfigPanel)
        panel.indicator_vars = {
            'pending_rows': _DummyVar(),
            'moved_rows': _DummyVar(),
            'last_post_succeeded_at': _DummyVar(),
        }

        panel._update_indicators_from_result({})

        self.assertEqual(panel.indicator_vars['pending_rows'].get(), '-')
        self.assertEqual(panel.indicator_vars['moved_rows'].get(), '-')
        self.assertEqual(panel.indicator_vars['last_post_succeeded_at'].get(), '-')

    def test_format_indicator_datetime_to_brazil(self):
        from ui_panel import format_indicator_datetime_br

        self.assertEqual(
            format_indicator_datetime_br('2026-04-08T16:30:00Z'),
            '08/04/2026 13:30:00',
        )

    def test_format_indicator_datetime_keeps_dash_for_empty(self):
        from ui_panel import format_indicator_datetime_br

        self.assertEqual(format_indicator_datetime_br(''), '-')
        self.assertEqual(format_indicator_datetime_br(None), '-')


if __name__ == '__main__':
    unittest.main()
