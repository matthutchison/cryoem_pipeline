from transitions import Machine
from monitor import FilePatternMonitor
from utilities import safe_copy_file, compress_file
import asyncio
import logging
import os
import pathlib
import shutil
import time

class Project():
    '''Overarching project controller
    '''

    def __init__(self, project, pattern, frames=1):
        self.project = project
        self.workflow = Workflow()
        self.async = AsyncWorkflowHelper()
        self.monitor = FilePatternMonitor(pattern)
        self.paths = {
                'local_root': '/tmp/' + str(project),
                'storage_root': '/mnt/moab/' + str(project)
                }
        self.frames = frames

    def start(self):
        self.async.loop.run_until_complete(self._async_start())

    async def _async_start(self):
        while True:
            items = await self.monitor
            for item in items:
                model = WorkflowItem(item)
                self.workflow.add_model(model)
                model.initialize()
            await asyncio.sleep(self.workflow.MIN_IMPORT_INTERVAL)

class Workflow(Machine):
    '''The workflow state machine.
    '''
    MIN_IMPORT_INTERVAL = 20

    def __init__(self):
        states = ['initial',
                'creating',
                'importing',
                'stacking',
                'compressing',
                'exporting',
                'processing',
                'cleaning',
                'finished']
        Machine.__init__(self,
                states=states,
                initial='initial',
                auto_transitions=False)
        self.add_transition('initialize', source='initial', dest='creating')
        self.add_transition('import_file', source='creating', dest='importing')
        self.add_transition('stack', source='importing', dest='stacking')
        self.add_transition('compress', source=['importing', 'stacking'],
                dest='compressing')
        self.add_transition('export', source='compressing', dest='exporting')
        self.add_transition('hold_for_processing', source='exporting',
                dest='processing')
        self.add_transition('clean', source=['processing', 'exporting'],
                dest='cleaning')
        self.add_transition('finalize', source='cleaning', dest='finished')

class WorkflowItem():
    '''A file that will join and proceed through the workflow.
    '''

    def __init__(self, path, workflow, project):
        self.history = []
        self.files = {'original': pathlib.Path(path)}
        self.project = project
        self.workflow = workflow

    def _delta_mtime(self, path):
        '''Return the difference between system time and file modified timestamp
        '''
        return int(time.time()) - os.stat(path).st_mtime

    def _is_processing_complete(self, path):
        raise NotImplementedError

    def on_enter_creating(self):
        '''Check that the file has finished creation, then transition state

        Since we're using network file systems here, we're using a simple check
        to see if the file has been modified recently instead of something 
        fancier like inotify.
        '''
        dt = _delta_mtime(self.path)
        self.import_file() if dt > 15 else self.async.add_timed_callback(
                on_enter_creating, 16 - dt)

    def on_enter_importing(self):
        '''Copy (import) the file to local storage for processing.
        '''
        self.files['local_original'] = pathlib.Path(
                self.project.paths['local_root'],
                self.files['original'].name)
        safe_copy_file(self.files['original'],
                self.files['local_original'])
        if self.project.frames > 1:
            self.stack()
        else:
            self.files['local_stack'] = self.files['local_original']
            self.compress()
        
    def on_enter_stacking(self):
        '''Stack the files if the stack parameter evaluates True.

        Cycle until all of the relevant files are copied local, then stack and
        export a single time.
        '''

        #TODO: add stacking code
        raise NotImplementedError

    def on_enter_compressing(self):
        self.async.create_task(compress_file(self.files['local_stack']),
            done_cb=_compressing_cb)
        self.files['local_compressed'] = self.files['local_stack'].with_suffix(
                self.files['local_stack'].suffix + '.bz2')

    def _compressing_cb(self, fut):
        self.export() if not fut.exception() else self.compress()

    def on_enter_exporting(self):
        '''Export (copy) the compressed file to the storage location
        '''
        self.files['storage_final'] = pathlib.Path(
                self.project.paths['storage_root'],
                self.files['local_compressed'])
        safe_copy_file(self.files['local_compressed'],
                self.files['storage_final'])
        self.hold_for_processing()

    def on_enter_processing(self):
        '''Maintain processing state until scipion processing is complete.

        Watch for the indicators that the entire scipion processing stack has
        completed. Until then, recurse back to this state entrance. Once it has
        completed, proceed to clean up.
        '''
        if self._is_processing_complete(self.files['local_stack']):
            self.clean()
        else:
            self.async.add_timed_callback(on_enter_processing, 10)

    def on_enter_cleaning(self):
        raise NotImplementedError

    def on_enter_finished(self):
        raise NotImplementedError

class AsyncWorkflowHelper():
    '''Processes async calls for the workflow
    '''

    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def create_task(self, coro, done_cb=None):
        task = self.loop.create_task(coro)
        if done_cb:
            task.add_done_callback(done_cb)

    def add_timed_callback(self, func, sleep):
        self.loop.create_task(self._wrap_timed_callback(func, sleep))
    
    async def _wrap_timed_callback(self, func, sleep):
        await asyncio.sleep(sleep)
        func()
