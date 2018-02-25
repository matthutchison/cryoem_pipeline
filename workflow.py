from transitions import Machine
from monitor import FilePatternMonitor
from utilities import safe_copy_file, compress_file
import asyncio
import os
import pathlib
import shutil
import time

class Project():
    '''Overarching project controller
    '''

    def __init__(self, project, pattern):
        self.project = project
        self.workflow = Workflow()
        self.async = AsyncWorkflowHelper()
        self.monitor = FilePatternMonitor(pattern)
        self.paths = {
                'project_root': '/tmp/' + str(project),
                'storage_root': '/mnt/moab/' + str(project)
                }

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
        self.add_transition('import', source='creating', dest='importing')
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

    def on_enter_creating(self):
        '''Check that the file has finished creation, then transition state

        Since we're using network file systems here, we're using a simple check
        to see if the file has been modified recently instead of something 
        fancier like inotify.
        '''
        dt = _delta_mtime(self.path)
        if dt > 15
            self.import()
        else:
            self.async.add_timed_callback(on_enter_creating, 16 - dt)

    def on_enter_importing(self):
        '''Copy (import) the file to local storage for processing.
        '''
        self.files['local_original'] = pathlib.Path(
                self.project.paths['local_storage'],
                self.files['original'].name)
        safe_copy_file(self.files['original'],
                self.files['local_original'])
        
    def on_enter_stacking(self):
        '''Stack the files if the stack parameter evaluates True.
        '''
        #TODO: add stacking code
        pass

    def on_enter_compressing(self):
        self.async.create_task(compress_file(self.files['local_stack']),
            done_cb=_compressing_cb)

    def _compressing_cb(self, fut):
        if fut.done():
            pass

    def on_enter_exporting(self):
        pass

    def on_enter_processing(self):
        pass

    def on_enter_cleaning(self):
        pass

    def on_enter_finished(self):
        pass

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
