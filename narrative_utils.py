from installed_clients.workspaceClient import Workspace
from installed_clients.execution_engine2Client import execution_engine2
from installed_clients.NarrativeMethodStoreClient import NarrativeMethodStore
from yaml import load, Loader
import os
import json

_DEBUG = os.environ.get("DEBUG")


def _debug_json(obj):
    if _DEBUG:
        print(json.dumps(obj, indent=2))


class NarrativeUtils:
    config = load(open("concierge.yaml").read(), Loader=Loader)
    ws = Workspace(config['Global']['ws_url'])
    ee = execution_engine2(config['Global']['ee2_url'])
    nms = NarrativeMethodStore(config['Global']['nms_url'])
    _EXEC_FIELDS = ['batch_id', 'batch_job', 'created', 'child_jobs', 'job_id',
                    'queued', 'retry_count', 'retry_ids', 'retry_saved_toggle',
                    'status', 'updated', 'user', 'wsid']
    dryrun = os.environ.get('DRYRUN')

    def create_bulk_import_app_cell(self, job_id):
        """
        Create a bulk app just from the job_id
        """
        job = self.ee.check_job({'job_id': job_id})
        job_list = job['child_jobs']
        job_list.append(job_id)
        jobs = self.ee.check_jobs({'job_ids': job_list})["job_states"]

       
        # Grab some IDs
        cell_id = jobs[0]['job_input']['narrative_cell_info']['cell_id']
        run_id = jobs[0]['job_input']['narrative_cell_info']['run_id']

        job_by_id = dict()
        # Create exec piece
        # TODO: combine with below
        jobs_by_id = {}
        apps = {}
        for job in jobs:
            jid = job['job_id']
            rec = {}
            rec['cell_id'] = cell_id
            for f in self._EXEC_FIELDS:
                if f in job:
                    rec[f] = job[f]
            rec['job_output'] = job.get('job_output', {})
            jobs_by_id[jid] = rec
            app_id = job['job_input']['app_id']
            # Tempory Fix for a screw up
            app_id = app_id.replace('upload_metagenome_fasta_gff_file', 'import_gff_fasta_as_metagenome_from_staging')
            if app_id not in apps:
                apps[app_id] = []
            apps[app_id].append(job)
            if jid == job_id:
                jstate = rec

        apps.pop('batch')
        # Get the specs for later
        specs_by_appid = {}
        for spec in self.nms.get_method_spec({'ids': list(apps.keys())}):
            specs_by_appid[spec['info']['id']] = spec 
      
        inputs = {}
        params = {}
        fileParamIds = {}
        otherParamIds = {}
        outputParamIds = {}
        specs = {}
        for app_id, jobs in apps.items():
            conf = self.config['Apps'][app_id]
            bname = conf['batch_name']
            inputs[bname] = {'appId': app_id, 'files': []}
            params[bname] = {'filePaths': []}
            params[bname]['params'] = conf['params']
            # Build inputs
            for job in jobs:
                for p in conf['file_params']:
                    fn = job['job_input']['params'][0][p]
                    inputs[bname]['files'].append(fn)
                param = {}
                for p in conf['filePaths']:
                    val = job['job_input']['params'][0][p]
                    param[p] = val
                params[bname]['filePaths'].append(param)
            fileParamIds[bname] = conf['filePaths']
            otherParamIds[bname] = list(conf['params'].keys())
            outputParamIds[bname] = conf['outputParamIds']
            specs[bname] = specs_by_appid[app_id]


        # Build up the app chunk
        app = {
            "fileParamIds": fileParamIds, # TODO
            "otherParamIds": otherParamIds, # TODO
            "outputParamIds": outputParamIds, # TODO
            "specs": specs, # TODO
            "tag": "release"
        }
        # TODO: Make the state look at the options
        bulkImportCell = {
           "app": app,
           "exec": { "jobState": jstate, "jobs": { "byId": jobs_by_id } },
           "inputs": inputs,
           "params": params,
           "state": {
             "params": { "gff_metagenome": "complete" },
             "selectedFileType": "gff_metagenome",
             "selectedTab": "viewConfigure",
             "state": "inProgress"
           },
           "user-settings": { "showCodeInputArea": False }
           }

        # TODO: Compute Dates
        dstr = "Wed, 20 Oct 2021 03:46:18 GMT"
        kbase = {
          "bulkImportCell": bulkImportCell,
          "attributes":{
             "created": dstr,
             "id": cell_id,
             "lastLoaded": dstr,
             "status": "new",
             "subtitle": "Import files into your Narrative as data objects",
             "title": "Import from Staging Area"
           },
          "cellState": { "toggleMinMax": "minimized" },
          "type": "app-bulk-import"
          }

        # Basic skelton
        cell = {
          "cell_type": "code",
          "execution_count": 1,
          "metadata": {"kbase": kbase},
          "outputs": [],
          "source": "#TODO"
          }
        _debug_json(cell)
        return cell

    #DEPRECATE
    def add_batch_cell(self, to_import, cell_id, run_id, job_id):
        """
        Append a batch input cell to the narrative.
        This requires:
        - the list of samples to import
        - The cell ID
        - The run ID (not used yet)
        - The job ID for the parent job.
        """

        # TODO: Try to generate everything from EE2 and NMS
        cell = json.load(open('bulk_import.json'))
        # TODO run_id
        cell['metadata']['kbase']['attributes']['id'] = cell_id

        flist = []
        fpaths = []
        for item in to_import:
            # build params
            for fo in item.files:
                flist.append(fo.fn)
            fpaths.append(item.params)
        blk = cell['metadata']['kbase']['bulkImportCell']
        blk['inputs']['gff_metagenome']['files'] = flist
        blk['params']['gff_metagenome']['filePaths'] = fpaths
        blk['exec']['jobs'] = {'byId': {}}
        js = self.ee.check_job({'job_id': job_id})
        job_list = js['child_jobs']
        job_list.append(job_id)
        jobs = self.ee.check_jobs({'job_ids': job_list})["job_states"]
        job_by_id = dict()
        # TODO: Can we just use the output from check_jobs?
        for job in jobs:
            job_by_id[job['job_id']] = job
        for job in job_list:
            jr = job_by_id[job]
            rec = {}
            rec['cell_id'] = cell_id
            for f in self._EXEC_FIELDS:
                if f in jr:
                    rec[f] = jr[f]
            rec['job_output'] = jr.get('job_output', {})
            blk['exec']['jobs']['byId'][job] = rec
            if job == job_id:
                jstate = rec
        blk['exec']['jobState'] = jstate
        ref = self.study.narrative_ref
        resp = self.ws.get_objects2({"objects": [{"ref": ref}]})
        narr = resp['data'][0]
        usermeta = narr['info'][10]
        narrdata = narr['data']
        narrdata['cells'].append(cell)
        obj = {
            "objid": self.study.narrative_id,
            "type": "KBaseNarrative.Narrative-4.0",
            "data": narrdata,
            "meta": usermeta
            }
        if _DEBUG:
            json.dump(cell, open('debug.json', 'w'), indent=2)
        resp = self.ws.save_objects({"id": self.study.wsid, "objects": [obj]})

    def create_app_cell(self, job_id):
        """
        This method generates an app cell and appends it to the narrative given
        the job ID. It constructs the state and parameters from EE2 and NMS.
        Input: job_id
        """

        job = self.ee.check_job({'job_id': job_id})
        cell_id = job['job_input']['narrative_cell_info']['cell_id']
        run_id = job['job_input']['narrative_cell_info']['run_id']
        app_id = job['job_input']['app_id']
        spec = self.nms.get_method_spec({'ids': [app_id]})[0]
        #_debug_json(spec)
        app = {
          "gitCommitHash": spec['info']['git_commit_hash'],
          "id": spec['info']['id'],
          "spec": spec,
          "tag": "release",
          "version": spec['info']['ver']
          }

        appCell = {
          "app": app,
          "exec": {"jobState": {},
                   "jobStateUpdated": time(),
                   "launchState": {
                     "cell_id": cell_id,
                     "event": "launched_job",
                     "event_at": datetime.utcnow().isoformat(),
                     "job_id": job_id,
                     "run_id": run_id
                   }
          },
          "fsm": {
            "currentState": {
              "mode": "processing",
              "stage": "queued"
            }
          },
          "output": { "byJob": {} },
          "params": job['job_input']['params'][0],
          "user-settings": { "showCodeInputArea": False }
          }
        # Cherry pick exec params
        for f in self._EXEC_FIELDS:
            if f in job:
               appCell['exec']['jobState'][f] = job[f]

        # TODO: Compute dates
        kbase = {
          "appCell":appCell,
          "attributes":{
             "created": "Wed, 20 Oct 2021 03:46:18 GMT",
             "id": cell_id,
             "info": {
               "label": "more...",
               "url": "/#appcatalog/app/%s/release" % (app_id)
             },
             "lastLoaded": "Wed, 20 Oct 2021 03:46:18 GMT",
             "status": "new",
             "subtitle": spec['info']['subtitle'],
             "title": spec['info']['name']
           },
          "cellState": { "toggleMinMax": "minimized" },
          "type": "app"
          }

        cell = {
          "cell_type": "code",
          "execution_count": 1,
          "metadata": {"kbase": kbase},
          "outputs": [],
          "source": "#TODO"
          }
        return cell

    def append_cell(self, cell, ref):
        resp = self.ws.get_objects2({"objects": [{"ref": ref}]})
        narr = resp['data'][0]
        # We could get these from the ref but since we have them
        objid = narr['info'][0]
        wsid = narr['info'][6]
        usermeta = narr['info'][10]
        narrdata = narr['data']
        narrdata['cells'].append(cell)
        # TODO: Increment app counts in usermeta block
        obj = {
            "objid": objid,
            "type": "KBaseNarrative.Narrative-4.0",
            "data": narrdata,
            "meta": usermeta
            }
        if self.dryrun:
            return
        resp = self.ws.save_objects({"id": wsid, "objects": [obj]})
        _debug_json(resp)
        return resp

    def append_to_log(self, ref, text):
        """
        Append to text the Log cell of the narrative.
        WIP.
        """

        # Read the narrative
        resp = self.ws.get_objects2({"objects": [{"ref": ref}]})
        narr = resp['data'][0]
        # We could get these from the ref but since we have them
        objid = narr['info'][0]
        wsid = narr['info'][6]
        usermeta = narr['info'][10]
        narrdata = narr['data']
        modified = False
        for cell in narrdata['cells']:
            if cell['source'].startswith("# Log"):
                cell['source'] += '%s\n' % (text)
                modified = True

        if modified:
            obj = {
                "objid": objid,
                "type": "KBaseNarrative.Narrative-4.0",
                "data": narrdata,
                "meta": usermeta
                }
            resp = self.ws.save_objects({"id": wsid, "objects": [obj]})
        return modified
