import unittest
from pathlib import Path

from streaming.gold_queries import load_queries
from streaming.lakehouse import LakehousePaths


class LakehousePathsTests(unittest.TestCase):
    def test_medallion_paths_are_stable(self):
        paths = LakehousePaths('/data/streaming/')

        self.assertEqual(paths.bronze_events, '/data/streaming/bronze/events')
        self.assertEqual(paths.silver_events, '/data/streaming/silver/events')
        self.assertEqual(paths.silver_rejected_events,
                         '/data/streaming/silver/rejected_events')
        self.assertEqual(paths.silver_current_shipments,
                         '/data/streaming/silver/current_shipments')
        self.assertEqual(paths.gold_table('fact_desempeno_envio'),
                         '/data/streaming/gold/fact_desempeno_envio')
        self.assertEqual(paths.checkpoint, '/data/streaming/checkpoints/main')

    def test_gold_table_rejects_path_traversal(self):
        paths = LakehousePaths('/data/streaming')

        for invalid_name in ('', '../silver', 'nested/table', 'nested\\table'):
            with self.subTest(invalid_name=invalid_name):
                with self.assertRaises(ValueError):
                    paths.gold_table(invalid_name)

    def test_compliance_queries_are_named_and_reviewable(self):
        query_file = Path(__file__).parent.parent / 'queries' / 'gold_compliance.sql'

        queries = load_queries(query_file)

        self.assertEqual(set(queries), {
            'overall_compliance',
            'compliance_by_route',
            'late_shipments_by_destination',
            'high_risk_backlog',
            'center_dwell',
            'daily_route_indicators',
        })
        self.assertTrue(all(query.lstrip().upper().startswith('SELECT')
                            for query in queries.values()))


if __name__ == '__main__':
    unittest.main()
