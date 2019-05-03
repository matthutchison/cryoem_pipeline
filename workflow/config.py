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
        self.validators = self._get_default_validators()
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

    def prompt_user_configs(self):
        user_configs = [c for c in pathlib.Path(
                        APPLICATION_PATH, 'config/user').glob('*.json')]
        print(APPLICATION_PATH)
        print('Which configurations would you like to load?')
        print('\n'.join(('(%i) %s' % e for e in enumerate(user_configs))))
        choices = [user_configs[int(c.strip())] for c in
                   input('Choices (separate multiple with commas): ')
                   .split(sep=',')]
        for choice in choices:
            self.load(pathlib.Path(APPLICATION_PATH, 'config/user', choice))

    def prompt_for_unset_values(self):
        def fix_special(v):
            if v.lower() in ['y', 'n', 'yes', 'no']:
                v = True if v.lower()[0] == 'y' else False
            else:
                try:
                    v = int(v)
                except ValueError:
                    try:
                        v = float(v)
                    except ValueError:
                        pass
            return v
        for k, v in self.config_options.items():
            if v is None:
                new = input('Set value for %s?: ' % k)
                if new:
                    self.config_options[k] = fix_special(new)

    def reload(self):
        config = self._load_config_file(self.path)
        self.config_options = config

    def save(self, path=None, force=False):
        if path:
            self.path = str(path)
        if os.path.exists(self.path) and not force:
            f = input('Config save file %s exists, overwrite (y/n)? ' %
                      self.path)
            if len(f) > 0 and f[0] in ['y', 'Y']:
                force = True
        with open(self.path, mode='w' if force else 'x', encoding='utf8') as f:
            f.write(json.dumps(self.config_options))

    def validate(self, key, validators):
        '''Validate the configuration option 'key' in self.config_options
        '''
        for validator in validators:
            try:
                if not validator(self.config_options[key]):
                    print('Configuration check failed for %s with value %s' %
                          (key, self.config_options[key]), file=sys.stderr)
                    return False
            except KeyError:
                logger.info('Did not validate %s, config option not found' %
                            key)
        return True

    def validate_all(self):
        return all((self.validate(k, v) for k, v in self.validators.items()))

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

    def _get_default_validators(self):
        return {
            'system_logger': [
                lambda v: bool(v),
                lambda v: pathlib.Path(v).exists()],
            'working_directory': [
                lambda v: bool(v),
                lambda v: pathlib.Path(v).exists()],
            'scipion_template_path': [
                lambda v: bool(v),
                lambda v: pathlib.Path(v).exists()],
            'run_config_directory': [
                lambda v: bool(v),
                lambda v: pathlib.Path(v).exists()],
            'globus_source_endpoint_id': [
                lambda v: v is None or bool(UUID(v))],
            'globus_destination_endpoint_id': [
                lambda v: v is None or bool(UUID(v))],
            'globus_source_endpoint_path': [
                lambda v: v is None or pathlib.Path(v)],
            'globus_destination_endpoint_path': [
                lambda v: v is None or pathlib.Path(v)],
            'project_name': [
                lambda v: bool(v)],
            'gain_reference_path': [
                lambda v: bool(v),
                lambda v: pathlib.Path(v).exists()],
            'source_pattern': [
                lambda v: bool(v),
                lambda v: len(glob(v), recursive=True) > 0],
            'frames_to_stack': [
                lambda v: bool(v),
                lambda v: 0 < v < 100],
            'physical_pixel_size': [
                lambda v: bool(v),
                lambda v: 1 < v < 50],
            'image_pixel_size': [
                lambda v: bool(v),
                lambda v: 0.1 < v < 5],
            'super_resolution': [
                lambda v: v in (True, False)],
            'ctf_low_resolution': [
                lambda v: 1 < v < 50],
            'ctf_high_resolution': [
                lambda v: 1 < v < 50],
            'defocus_search_minimum': [
                lambda v: 0 < v < 10],
            'defocus_search_maximum': [
                lambda v: 0 < v < 100],
            'scipion_run_template_path': [
                lambda v: bool(v),
                lambda v: not pathlib.Path(v).exists()]
        }


class ScipionTemplate():
    '''Container for project configuration values
    '''
    def __init__(self, config):
        if config.validate_all():
            self.config = config.config_options
            self._set_keyword_values()
        else:
            sys.exit('Configuration not validated prior to template generation.\
                     exiting.')

    def _set_keyword_values(self):
        config = self.config
        self.project_name = config.get('project_name')
        self.source_pattern = config.get('source_pattern')
        self.working_directory = config.get('working_directory')
        self.path_to_gainref = config.get('gain_reference_path')
        self.frames_to_stack = config.get('frames_to_stack')
        self.physical_pixel_size = config.get('physical_pixel_size')
        self.image_pixel_size = config.get('image_pixel_size')
        self.super_resolution = config.get('super_resolution')
        self.ctf_low_res = config.get('ctf_low_resolution')
        self.ctf_high_res = config.get('ctf_high_resolution')
        self.defocus_search_min = config.get('defocus_search_minimum')
        self.defocus_search_max = config.get('defocus_search_maximum')
        self.voltage = config.get('voltage')
        self.scipion_config_path = pathlib.Path(
            config.get('run_config_directory'),
            config.get('project_name') + '-scipion-template.json')

    def generate_template(self):
        self.get_config_values()
        self.load_template(pathlib.Path(
            APPLICATION_PATH,
            self.config.get('scipion_template_path')))
        self.template_insert_values()
        self.write_template(self.scipion_config_path)

    def load_template(self, template_path):
        self.template = self._load_template(template_path)

    @staticmethod
    def _load_template(path):
        '''Loads the Scipion JSON template from path.
        '''
        if not os.path.exists(str(path)):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path)
        with open(str(path), mode='r', encoding='utf8') as f:
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
        if os.path.exists(str(path)) and not force:
            raise FileExistsError(
                errno.EEXIST,
                os.strerror(errno.EEXIST),
                path)
        with open(str(path), mode='w', encoding='utf8') as f:
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
        imp['voltage'] = config.voltage

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
            input('Physical pixel size (5?) (µm): '))
        config.image_pixel_size = float(
            config.image_pixel_size or
            input('Image pixel size (Å): '))
        config.super_resolution = (
            config.super_resolution if config.super_resolution is not None else
            (True if input('Are you running super resolution (y/n): ')[0] in
                ('y', 'Y') else False))
        config.ctf_low_res = float(
            config.ctf_low_res or
            input('CTF search low resolution bound (30?) (Å): '))
        config.ctf_high_res = float(
            config.ctf_high_res or
            input('CTF search high resolution bound (3?) (Å): '))
        config.defocus_search_min = float(
            config.defocus_search_min or
            input('Defocus search minimum (.25?) (µm): '))
        config.defocus_search_max = float(
            config.defocus_search_max or
            input('Defocus search maximum (5?) (µm): '))
        config.scipion_config_path = (
            config.scipion_config_path or
            input('Path to save scipion template: '))
