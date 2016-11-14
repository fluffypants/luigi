# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an integration test for the BigQuery-luigi binding.

This test requires credentials that can access GCS & access to a bucket below.
Follow the directions in the gcloud tools to set up local credentials.
"""

import json
import os

import luigi
from luigi.contrib import bigquery, bigquery_avro, gcs
import oauth2client
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from contrib import gcs_test
from nose.plugins.attrib import attr

PROJECT_ID = gcs_test.PROJECT_ID
DATASET_ID = os.environ.get('BQ_TEST_DATASET_ID', 'luigi_tests')
CREDENTIALS = oauth2client.client.GoogleCredentials.get_application_default()


@attr('gcloud')
class TestLoadTask(bigquery.BigQueryLoadTask):
    source = luigi.Parameter()
    table = luigi.Parameter()

    @property
    def schema(self):
        return [
            {'mode': 'NULLABLE', 'name': 'field1', 'type': 'STRING'},
            {'mode': 'NULLABLE', 'name': 'field2', 'type': 'INTEGER'},
        ]

    def source_uris(self):
        return [self.source]

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table)


@attr('gcloud')
class TestRunQueryTask(bigquery.BigQueryRunQueryTask):
    query = ''' SELECT 'hello' as field1, 2 as field2 '''
    table = luigi.Parameter()

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table)


@attr('gcloud')
class BigQueryGcloudTest(gcs_test._GCSBaseTestCase):
    def setUp(self):
        super(BigQueryGcloudTest, self).setUp()
        self.bq_client = bigquery.BigQueryClient(gcs_test.CREDENTIALS)

        self.table = bigquery.BQTable(project_id=PROJECT_ID, dataset_id=DATASET_ID,
                                      table_id=self.id().split('.')[-1])
        self.addCleanup(self.bq_client.delete_table, self.table)

    def create_dataset(self, data=[]):
        self.bq_client.delete_table(self.table)

        text = '\n'.join(map(json.dumps, data))
        gcs_file = gcs_test.bucket_url(self.id())
        self.client.put_string(text, gcs_file)

        task = TestLoadTask(source=gcs_file, table=self.table.table_id)

        task.run()

    def test_table_uri(self):
        intended_uri = "bq://" + PROJECT_ID + "/" + \
                       DATASET_ID + "/" + self.table.table_id
        self.assertTrue(self.table.uri == intended_uri)

    def test_load_and_copy(self):
        self.create_dataset([
            {'field1': 'hi', 'field2': 1},
            {'field1': 'bye', 'field2': 2},
        ])

        # Cram some stuff in here to make the tests run faster - loading data takes a while!
        self.assertTrue(self.bq_client.dataset_exists(self.table))
        self.assertTrue(self.bq_client.table_exists(self.table))
        self.assertIn(self.table.dataset_id,
                      list(self.bq_client.list_datasets(self.table.project_id)))
        self.assertIn(self.table.table_id,
                      list(self.bq_client.list_tables(self.table.dataset)))

        new_table = self.table._replace(table_id=self.table.table_id + '_copy')
        self.bq_client.copy(
            source_table=self.table,
            dest_table=new_table
        )
        self.assertTrue(self.bq_client.table_exists(new_table))
        self.bq_client.delete_table(new_table)
        self.assertFalse(self.bq_client.table_exists(new_table))

    def test_run_query(self):
        task = TestRunQueryTask(table=self.table.table_id)
        task._BIGQUERY_CLIENT = self.bq_client
        task.run()

        self.assertTrue(self.bq_client.table_exists(self.table))


@attr('gcloud')
class BigQueryLoadAvroTest(unittest.TestCase):
    def _produce_test_input(self):
        schema = avro.schema.parse("""{
            "name": "TestQueryTask_record",
            "type": "record",
            "doc": "The description",
            "fields": [
                {"name": "col0", "type": "int", "doc": "The bold"},
                {"name": "col1", "type": "int", "doc": "The beautiful"}
            ]
        }""")
        writer = DataFileWriter(open("tmp.avro", "wb"), DatumWriter(), schema)
        writer.append({'col0': 1000, 'col1': 1001})
        writer.close()
        # FIXME don't need a tmp.avro; use GCS API like objects().insert that accepts file contents in a string:
        # https://developers.google.com/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#insert

    def setUp(self):
        self.gcs_client = gcs.GCSClient(CREDENTIALS).client
        self.bq_client = bigquery.BigQueryClient(CREDENTIALS).client

        self.table_id = self.id().split('.')[-1]
        self.gcs_dir_url = 'gs://' + self.id() +
        # self.addCleanup(self.bq_client.delete_table, self.table)
        # self.addCleanup(self.gcs_client.)

    def test_load_avro_dir_and_propagate_doc(self):
        class BigQueryLoadAvroTestInput(luigi.ExternalTask):
            def output(_):
                return gcs.GcsTarget(self.gcs_dir_url)

        class BigQueryLoadAvroTestTask(bigquery_avro.BigQueryLoadAvro):
            def requires(_):
                return DummyGcsInput()

            def output(_):
                return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table_id)

        task = BigQueryLoadAvroTestTask()
        self.assertFalse(task.complete())
        task.run()
        self.assertTrue(task.complete())

        table = self.bq_client.dataset(DATASET_ID).table(self.table_id)
        self.assertTrue(table.exists)
        table.reload()
        self.assertEqual(table.description, 'The description')
