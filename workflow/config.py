from glob import glob
from uuid import UUID
import errno
import logging
import json
import os
import pathlib
import sys

APPLICATION_PATH = os.path.realpath(sys.path[0])
logger = logging.getLogger(__name__)


class Config():
    '''Pipeline configuration for the current run.

    Must contain all of the appropriate user-defined and system values needed
    for the application to run.  Where those are can be distributed amongst
    files or initially input on the command line, but will be centralized in
    this configuration and then saved out to a file which can be reloaded
    during the run to adjust values on the fly.

    '''

    def __init__(self):
        self.config_options = dict()
        self.validators = dict()
        self.path = None

    def __str__(self):
        from pprint import pformat
        return pformat(self.config_options)

    def load(self, paths):
        '''Load configuration files and merge the dictionaries in order.

        In a list of dicts [x, y, z], the values loaded from config z will
        override the values from configs x and y.
        '''
        if isinstance(paths, str) or isinstance(paths, pathlib.PurePath):
            paths = [paths]
        for path in paths:
            conf = self._load_config_file(path)
            self.config_options = {**self.config_options, **conf}

    def reload(self):
        config = self._load_config_file(self.path)
        self.config_options = config

    @staticmethod
    def _load_config_file(path):
        path = str(path)
        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path)
        with open(path, mode='r', encoding='utf8') as f:
            try:
                config = json.loads(f.read())
            except json.JSONDecodeError as jde:
                logger.warning('Could not load config file\n%s' % jde)
        return config

    def save(self, force=False):
        if os.path.exists(self.path) and not force:
            f = input('Config save file %s exists, overwrite (y/n)? ' %
                      self.path)
            if len(f) > 0 and f[0] in ['y', 'Y']:
                force = True
        with open(self.path, mode='w' if force else 'x', encoding='utf8') as f:
            f.write(json.dumps(self.config_options))

    def validate(self, key, validators):
        try:
            return all((v(self.config_options[key]) for v in validators))
        except KeyError:
            logger.info('Did not validate %s, config option not found' % key)

    def validate_all(self):
        return all((self.validate(k, v) for k, v in self.validators.items()))


class SystemConfig():
    '''Container for system configuration values
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_keyword_values(self, **kwargs):
        self.working_directory = kwargs.get('working_directory')
        self.src_globus_id = kwargs.get('src_endpoint_id')
        self.src_globus_path = kwargs.get('src_endpoint_path')
        self.dest_globus_id = kwargs.get('dest_endpoint_id')
        self.dest_globus_path = kwargs.get('dest_endpoint_path')
        self.logging_path = kwargs.get('log_file')
        self.default_template = kwargs.get('default_scipion_template_path')

    def get_config_values(self):
        self._get_config_values(self)
        self.validate_config()

    @staticmethod
    def _get_config_values(config):
        config.working_directory = (
            config.working_directory or
            input('Working directory: '))
        config.src_globus_id = (
            config.src_globus_id or
            input('Source globus endpoint ID: '))
        config.src_globus_path = (
            config.src_globus_path or
            input('Source globus path: ') or
            '/')
        config.dest_globus_id = (
            config.dest_globus_id or
            input('Destination globus endpoint ID: '))
        config.dest_globus_path = (
            config.dest_globus_path or
            input('Destination globus path: ') or
            '/')
        config.logging_path = (
            config.logging_path or
            input('Logging path: '))
        config.default_template = (
            config.default_template or
            input('Path to default scipion template: '))

    def validate_config(self):
        self._validate_config(self)

    @staticmethod
    def _validate_config(config):
        _v_wrap = lambda x: x
        return (all([
            _v_wrap(not config.working_directory,
                    'Working dir blank'),
            _v_wrap(not pathlib.Path(config.working_directory).exists(),
                    'Work dir %s does not exist' % config.working_directory),
            _v_wrap(not config.logging_path,
                    'Working dir blank'),
            _v_wrap(not pathlib.Path(config.logging_path).exists(),
                    'Work dir %s does not exist' % config.logging_path),
            _v_wrap(not config.default_template,
                    'Working dir blank'),
            _v_wrap(not pathlib.Path(config.default_template).exists(),
                    'Work dir %s does not exist' % config.working_directory),
            _v_wrap(not UUID(config.dest_globus_id),
                    'No Globus destination ID'),
            _v_wrap(not UUID(config.src_globus_id),
                    'No Globus source ID'),
        ]))


class ScipionConfig():
    '''Container for project configuration values
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_keyword_values(self, **kwargs):
        self.project_name = kwargs.get('project')
        self.source_pattern = kwargs.get('src_pattern')
        self.path_to_gainref = kwargs.get('path_to_gainref')
        self.frames_to_stack = kwargs.get('frames')
        self.physical_pixel_size = kwargs.get('physical_pixel')
        self.image_pixel_size = kwargs.get('image_pixel')
        self.super_resolution = kwargs.get('super_resolution')
        self.ctf_low_res = kwargs.get('ctf_low_res')
        self.ctf_high_res = kwargs.get('ctf_high_res')
        self.defocus_search_min = kwargs.get('defocus_min')
        self.defocus_search_max = kwargs.get('defocus_max')
        self.scipion_config_path = kwargs.get('scipion_output')

    def generate_config(self):
        self.get_config_values()
        if not self.validate_config():
            sys.exit('Scipion configuration validation failed.')
        self.load_template(
            '%s/workflow/workflow_template.json' %
            APPLICATION_PATH)
        self.template_insert_values()
        self.write_template(self.scipion_config_path)

    def load_template(self, template_path):
        self.template = self._load_template(template_path)

    @staticmethod
    def _load_template(path):
        '''Loads the Scipion JSON template from path.
        '''
        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path)
        with open(path, mode='r', encoding='utf8') as f:
            try:
                temp = json.loads(f.read())
            except json.JSONDecodeError:
                sys.exit(1)
        return temp

    def write_template(self, output_path=None, force=False):
        self._write_template(self.template, output_path, force)

    @staticmethod
    def _write_template(data, path, force):
        '''Writes the JSON template out to the provided path.

        Should only be called after filling the template with data.
        '''
        if os.path.exists(path) and not force:
            raise FileExistsError(
                errno.EEXIST,
                os.strerror(errno.EEXIST),
                path)
        with open(path, mode='w', encoding='utf8') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))
            f.close()

    def template_insert_values(self):
        self._template_insert_values(self.template, self)

    @staticmethod
    def _template_insert_values(template, config):
        '''Inserts parameters into the scipion JSON template.
        '''
        imp = template[0]
        ctf = template[2]
        imp['filesPath'] = str(pathlib.Path(
                            config.working_directory,
                            config.project_name))
        imp['filesPattern'] = ('stack/*.mrc' if config.frames_to_stack > 1
                               else '*.mrc')
        imp['magnification'] = (((config.physical_pixel_size * .000001) /
                                (config.image_pixel_size * .0000000001)) /
                                (2 if config.super_resolution else 1))
        imp['samplingRate'] = config.image_pixel_size
        imp['scannedPixelSize'] = config.physical_pixel_size
        imp['gainFile'] = config.path_to_gainref

        ctf['minDefocus'] = config.defocus_search_min
        ctf['maxDefocus'] = config.defocus_search_max
        ctf['lowRes'] = config.image_pixel_size / config.ctf_low_res
        ctf['highRes'] = config.image_pixel_size / config.ctf_high_res

    def get_config_values(self):
        self._get_config_values(self)

    @staticmethod
    def _get_config_values(config):
        '''Prompt the user for any expected configuration values that have
        not already been set.
        '''
        config.project_name = (
            config.project_name or
            input('Project name: '))
        config.path_to_gainref = (
            config.path_to_gainref or
            input('Path to gain reference file (local machine): '))
        config.source_pattern = (
            config.source_pattern or
            input('Pattern to match files for import (ex /path/to/*.mrc): '))
        config.frames_to_stack = int(
            config.frames_to_stack or
            input('Frames to stack (1 for prestacked): '))
        config.physical_pixel_size = float(
            config.physical_pixel_size or
            input('Physical pixel size (5?) (um): '))
        config.image_pixel_size = float(
            config.image_pixel_size or
            input('Image pixel size (A): '))
        _super_res = (
            config.super_resolution or
            input('Are you running super resolution (y/n): '))
        config.super_resolution = (True if
                                   _super_res is True or
                                   _super_res[0] in ('y', 'Y')
                                   else False)
        config.ctf_low_res = float(
            config.ctf_low_res or
            input('CTF search low resolution bound (30?) (A): '))
        config.ctf_high_res = float(
            config.ctf_high_res or
            input('CTF search high resolution bound (3?) (A): '))
        config.defocus_search_min = float(
            config.defocus_search_min or
            input('Defocus search minimum (.25?) (um): '))
        config.defocus_search_max = float(
            config.defocus_search_max or
            input('Defocus search maximum (5?) (um): '))
        config.scipion_config_path = (
            config.scipion_config_path or
            '/mnt/nas/Scipion/'+config.project_name+'.json')

    def validate_config(self):
        '''Validate the class configuration.
        '''
        return self._validate_config(self)

    @staticmethod
    def _validate_config(config):
        '''Validate the provided application configuration.

        This is always intended to be called *before* the Scipion configuration
        is written and Scipion process started. Failing to do so will result in
        untested configurations being allowed to kick off scipion processes of
        unknown validity.

        The numeric validations here are for sanity-check purposes and will
        not protect the user from making bad but potentially reasonable
        choices. File path checks are more firm, making sure that things like
        the gain reference and target directories exist.
        '''
        _v_wrap = lambda x: x
        return(all([
            _v_wrap(not config.project_name,
                    'Project name blank'),
            _v_wrap(not config.path_to_gainref,
                    'Path to gain ref blank'),
            _v_wrap(not pathlib.Path(config.path_to_gainref).exists(),
                    'Path to gain ref does not exist'),
            _v_wrap(not config.source_pattern,
                    'Source pattern is blank'),
            _v_wrap(not(len(glob(config.source_pattern, recursive=True)) > 0),
                    'No files matching pattern %s' % config.source_pattern),
            _v_wrap(not config.frames_to_stack > 0,
                    'Frames to stack out of range 1-100'),
            _v_wrap(not config.frames_to_stack < 100,
                    'Frames to stack out of range 1-100'),
            _v_wrap(not config.physical_pixel_size >= 1,
                    'Physical pixel size out of range 1-50'),
            _v_wrap(not config.physical_pixel_size <= 50,
                    'Physical pixel size out of ramge 1-50'),
            _v_wrap(not config.image_pixel_size < 5,
                    'Image pixel size out of range 0.1-5'),
            _v_wrap(not config.image_pixel_size > 0.1,
                    'Image pixel size out of range 0.1-5'),
            _v_wrap(config.super_resolution not in (True, False),
                    'Super resolution value invalid. Valid are True, False.'),
            _v_wrap(not config.ctf_low_res < 50,
                    'CTF low resolution out of range 1-50'),
            _v_wrap(not config.ctf_low_res > 1,
                    'CTF low resolution out of range 1-50'),
            _v_wrap(not config.ctf_high_res < 50,
                    'CTF high resolution out of range 1-50'),
            _v_wrap(not config.ctf_high_res > 1,
                    'CTF high resolution out of range 1-50'),
            _v_wrap(config.ctf_low_res < config.ctf_high_res,
                    'CTF high resolution lower than CTF low resolution'),
            _v_wrap(not config.defocus_search_min > 0,
                    'Defocus search minimum out of range 0-10'),
            _v_wrap(not config.defocus_search_min < 10,
                    'defocus search minimum out of range 0-10'),
            _v_wrap(not config.defocus_search_max > 0,
                    'Defocus search maximum out of range 0-100'),
            _v_wrap(not config.defocus_search_max < 100,
                    'Defocus search maximum out of range 0-100'),
            _v_wrap(config.defocus_search_min > config.defocus_search_max,
                    'Defocus search min higher than defocus search max'),
            _v_wrap(config.scipion_config_path is None,
                    'Scipion configuration path could not be generated'),
            _v_wrap(pathlib.Path(config.scipion_config_path).exists(),
                    'Scipion configuration file %s already exists.\
                    select a unique project name.' %
                    config.scipion_config_path),
        ]))

# TODO: commandline specs, prompting
# TODO: validators
# TODO: actually use it
