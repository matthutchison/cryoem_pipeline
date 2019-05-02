import unittest
import pathlib
from unittest.mock import patch
from workflow.config import Config, APPLICATION_PATH
from workflow.workflow import Project


class BasicIntegrationTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_default_config(self):
        c = Config()
        c.load(pathlib.Path(
               APPLICATION_PATH, 'config/default/base_system_config.json'))
        c.load(pathlib.Path(
               APPLICATION_PATH, 'config/system').glob('*.json'))
        c.config_options.update({'project_name': 'test_integration_tests'})
        return c

    def test_project_initializes(self):
        config = self.get_default_config()
        with patch.object(Project,
                          '_verify_globus_activation', lambda e: -1) as P:
            project = P(config)
        del project

    def test_config_initializes(self):
        config = self.get_default_config()
        del config
