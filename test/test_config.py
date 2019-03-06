import workflow.config as conf
import pathlib
import unittest
import tempfile

nt = tempfile.NamedTemporaryFile


class ConfigTests(unittest.TestCase):

    fake_path = '/path/to/a/fake/file'

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_save_and_reload_identical_data(self):
        config = conf.Config()
        config.config_options['test1'] = 'test value'
        config.config_options['test2'] = dict(test_a='test value 1',
                                              test_b='test value 2')
        config.config_options['test_num'] = 3
        config.config_options['test_list'] = [1, 2, 3, 4, 5, 'value', 'val2']
        with nt(mode='w') as f:
            config.path = f.name
            copy = config.config_options.copy()
            config.save(force=True)
            config.config_options = dict()
            config.load(config.path)
            self.assertEqual(config.config_options, copy)
            copy['bad'] = 'bad'
            self.assertNotEqual(config.config_options, copy)

    def test_validation_good_succeeds(self):
        config = conf.Config()
        validators = [lambda x: x == 'test2', lambda x: True]
        config.validators.clear()
        config.validators['test1'] = validators
        config.config_options['test1'] = 'test2'
        self.assertTrue(config.validate('test1', validators))
        self.assertTrue(config.validate_all())

    def test_validation_bad_fails(self):
        config = conf.Config()
        validators = [lambda x: x == 'test2', lambda x: False]
        config.validators.clear()
        config.validators['test1'] = validators
        config.config_options['test1'] = 'test2'
        self.assertFalse(config.validate('test1', validators))
        self.assertFalse(config.validate_all())

    def test_load_single_and_multiple_is_consistent(self):
        config = conf.Config()
        config.load('config/default/base_system_config.json')
        config.load(['config/default/base_system_config.json',
                    'config/default/base_system_config.json'])

    def test_load_works_with_path_objects(self):
        config = conf.Config()
        config.load(pathlib.Path('config/default/base_system_config.json'))
        config.load([pathlib.Path('config/default/base_system_config.json'),
                     pathlib.Path('config/default/base_system_config.json')])
