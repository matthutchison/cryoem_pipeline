import json
import os
import pathlib
import sys


class Config():
    '''Container for project configuration values
    '''

    def __init__(self, **kwargs):
        self.project_name = kwargs.get('project')
        self.source_pattern = kwargs.get('src_pattern')
        self.working_directory = kwargs.get('working_directory')
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
        self.load_template('workflow/workflow_template.json')
        self.template_insert_values()
        self.write_template(self.scipion_config_path)

    def load_template(self, template_path):
        self.template = self._load_template(template_path)

    @staticmethod
    def _load_template(path):
        if not os.path.exists(path):
            raise FileNotFoundError
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
        if os.path.exists(path) and not force:
            raise FileExistsError
        with open(path, mode='w', encoding='utf8') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))
            f.close()

    def template_insert_values(self):
        self._template_insert_values(self.template, self)

    @staticmethod
    def _template_insert_values(template, config):
        imp = template[0]
        ctf = template[2]
        imp['filesPath'] = str(pathlib.Path(
                            config.working_directory,
                            config.project_name))
        imp['filesPattern'] = '*.mrc'
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
        config.project_name = (
            config.project_name or
            input('Project name: '))
        config.path_to_gainref = (
            config.path_to_gainref or
            input('Path to gain reference file (local machine): '))
        config.source_pattern = (
            config.source_pattern or
            input('Pattern to match files for import: '))
        config.working_directory = (
            config.working_directory or
            input('Scratch working directory (/tmp?): '))
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
            input('File to save scipion config?: '))
        return config

    def validate_config(self):
        return self._validate_config(self)

    @staticmethod
    def _validate_config(config):
        return(all([
            config.project_name is not None,
            config.path_to_gainref is not None,
            config.source_pattern is not None,
            config.working_directory is not None,
            pathlib.Path(config.working_directory).exists(),
            pathlib.Path(config.path_to_gainref).exists(),
            config.frames_to_stack > 0,
            config.frames_to_stack < 100,
            config.physical_pixel_size > 1,
            config.physical_pixel_size < 50,
            config.image_pixel_size < 5,
            config.image_pixel_size > 0.1,
            config.super_resolution in (True, False),
            config.ctf_low_res < 50,
            config.ctf_low_res > 1,
            config.ctf_high_res > 1,
            config.ctf_high_res < 50,
            config.ctf_low_res > config.ctf_high_res,
            config.defocus_search_min > 0,
            config.defocus_search_min < 10,
            config.defocus_search_max > 0,
            config.defocus_search_max < 100,
            config.scipion_config_path is not None
        ]))
