import requests
import json
import os
import sys
from installed_clients.workspaceClient import Workspace
from installed_clients.execution_engine2Client import execution_engine2
from installed_clients.SampleServiceClient import SampleService
from narrative_utils import NarrativeUtils
from yaml import load, Loader
from uuid import uuid4
from time import time
from datetime import datetime

_DEBUG = os.environ.get("DEBUG")


def _debug(text):
    if _DEBUG:
        print(text)

def _debug_json(obj):
    if _DEBUG:
        print(json.dumps(obj, indent=2))

class Concierge:
    """
    Study/Narrative: This is the main class that is used to
    create a narrative and manage its contents.
    """
    config = load(open("concierge.yaml").read(), Loader=Loader)
    _staging = config['Global']['staging_url']
    ws = Workspace(config['Global']['ws_url'])
    auth = config['Global']['auth_url']
    ss = SampleService(config['Global']['sample_service_url'])
    ee = execution_engine2(config['Global']['ee2_url'])
    nu = NarrativeUtils()

    dryrun = os.environ.get('DRYRUN')

    def __init__(self, study, transfer=None):
        """
        study: Study Object
        """
        self.study = study
        self.headers = {"Authorization": os.environ["KB_AUTH_TOKEN"]}
        # Get the username
        resp = requests.get('%s/me' % (self.auth), headers=self.headers).json()
        self.user = resp['user']
        self.transfer = transfer

    def sync(self):
        """
        This will be the main driver function to synchronize content
        """
        self.initialize_narrative()
        # TODO Add way to detect if samples need updating
        update_samples = False
        if update_samples:
            self.make_samples()
            self.submit_sample_import()
        self.find_new_data()
        self.link_objects()

    def make_samples(self):
        """
        Create the Sample TSV file for uploading to KBase
        """

        data = []
        headings = self.study.samples_headers
        data.append("\t".join(headings))
        for sample in self.study.samples:
            row = []
            sinfo = sample.data
            for c in headings:
                row.append(str(sinfo[c]))
            data.append('\t'.join(row))
        url = "%s/upload" % (self._staging)
        fn = os.path.join("/tmp", "%s.tsv" % (self.study.id))
        with open(fn, "w") as f:
            f.write('\n'.join(data))
        params = {'destPath': '/'}
        files = {"uploads": open(fn, "rb")}
        resp = requests.post(url, headers=self.headers,
                             data=params, files=files)
        return resp.json()

    def submit_sample_import(self):
        """
        Incomplete: Submit the sample import for execution
        """

        # TODO: Use the config for this.
        fn = "%s.tsv" % (self.study.id)
        params = {
            "sample_file": fn,
            "file_format": self.study.tmpl,
            "set_name": self.study.sampleset,
            "header_row_index": None,
            "name_field": "",
            "description": self.study.name,
            "sample_set_ref": None,
            "output_format": "",
            "taxonomy_source": "n/a",
            "num_otus": 20,
            "incl_seq": 0,
            "otu_prefix": "OTU",
            "incl_input_in_output": 1,
            "share_within_workspace": 1,
            "prevalidate": 1,
            "keep_existing_samples": 1,
            "ignore_warnings": 0,
            "workspace_name": self.study.workspace_name,
            "workspace_id": self.study.wsid
        }

        rpc = {
            "method": "sample_uploader.import_samples",
            "app_id": "sample_uploader/import_samples",
            "params": [params],
            "wsid": self.study.wsid
        }
        job_id = self.submit_app(rpc)
        self.add_app_cell(job_id)

    def submit_app(self, param):
        """
        Submit a prepare app to ee2
        """
        cell_id = str(uuid4())
        run_id = str(uuid4())

        param['meta'] = {"cell_id": cell_id, "run_id": run_id}
        if self.dryrun:
            return
        job_id = self.ee.run_job(param)
        return job_id

    def add_app_cell(self, job_id):
        """
        This method generates an app cell and appends it to the narrative given
        the job ID. It constructs the state and parameters from EE2 and NMS.
        Input: job_id
        """
        cell = self.nu.create_app_cell(job_id)
        ref = self.study.narrative_ref
        self.nu.append_cell(cell, ref)

        # resp = self.ws.get_objects2({"objects": [{"ref": ref}]})
        # narr = resp['data'][0]
        # usermeta = narr['info'][10]
        # narrdata = narr['data']
        # narrdata['cells'].append(cell)
        # obj = {
        #     "objid": self.study.narrative_id,
        #     "type": "KBaseNarrative.Narrative-4.0",
        #     "data": narrdata,
        #     "meta": usermeta
        #     }
        # if self.dryrun:
        #     return
        # resp = self.ws.save_objects({"id": self.study.wsid, "objects": [obj]})

    def get_staged_files(self):
        """
        Returns dictionary of staged files
        """

        url = "%s/list" % (self._staging)
        resp = requests.get(url, headers=self.headers)
        if resp.status_code != 200:
            sys.write.stderr(resp.text + '\n')
        staged = {}
        for fo in resp.json():
            staged[fo["name"]] = fo
        return staged

    def find_new_data(self):
        """
        Query what is in the workspace and compare
        that against the samples assoicated with the
        study.  Come up with what needs to be staged
        and imported
        """

        # Let's start with what do have
        done = {}
        for obj in self.ws.list_objects({'ids': [self.study.wsid]}):
            done[obj[1]] = 1

        # Now let's go through the samples and see what's missing
        missing = []
        for sample in self.study.data_objects:
            if sample.name not in done:
                missing.append(sample)

        staged = self.get_staged_files()
        to_stage = []
        to_import = []
        for item in missing:
            ready = True
            for fo in item.files:
                if fo.fn not in staged:
                    ready = False
                    print("Staging %s" % (fo.src))
                    to_stage.append(fo.src)

            if ready:
                item.staged = True
                print("Import %s" % (item.name))
                to_import.append(item)
        # TODO: Some way to see what is in flight
        if len(to_stage) > 0:
            self.transfer.transfer(to_stage)

        self.submit_import(to_import)

    def submit_import(self, to_import):
        """
        Create a bulk import job and append an app to the narrative.
        """
        if len(to_import) == 0:
            print("Nothing to import")
            return
        cell_id = str(uuid4())
        run_id = str(uuid4())
        job_meta = {"cell_id": cell_id, "run_id": run_id}

        # plist is the parameter list for the batch import
        plist = []
        for obj in to_import:
            print("Import: %s" % (obj.name))
            method = obj.method
            app_id = obj.appid
            # Set the workspace name
            obj.params["workspace_name"] = self.study.workspace_name

            param = {
                "method": method,
                "params": [obj.params],
                "app_id": app_id,
                "meta": job_meta,
            }
            plist.append(param)

        # Batch Param.. Just the workspace id
        bp = {'wsid': self.study.wsid}
        _debug(plist)
        _debug(bp)
        if self.dryrun:
            return
        resp = self.ee.run_job_batch(plist, bp)
        job_id = resp['batch_id']
        _debug(cell_id)
        _debug(job_id)
        cell = self.nu.create_bulk_import_app_cell(job_id)
        ref = self.study.narrative_ref
        self.nu.append_cell(cell, ref)
        # self.add_batch_cell(to_import, cell_id, run_id, job_id)

    def _get_sample_set_map(self):
        """
        Returns a map from the sample name to the KBase
        sample ID
        """
        # Build a map of sample names to sample IDs in KBase
        obj = {'wsid': self.study.wsid, 'name': self.study.sampleset}
        try:
            res = self.ws.get_objects2({'objects': [obj]})
        except:
            print("No sample set available yet.")
            return {}
        sam_name2obj = {}
        for s in res['data'][0]['data']['samples']:
            sam_name2obj[s['name']] = s
        return sam_name2obj

    def link_objects(self):
        """
        Link the imported objects to their samples.
        """
        # Get the sample data mapping
        sam_name2obj = self._get_sample_set_map()

        # Make a map of the ws object names to their UPA 
        obj_name2upa = {}
        for obj in self.ws.list_objects({'ids': [self.study.wsid]}):
            upa = "%d/%d/%d" % (obj[6], obj[0], obj[4])
            obj_name2upa[obj[1]] = upa

        linked_types = self.config['Linking']
        # So now let's go through all the objects in the study
        for obj in self.study.data_objects:
            _debug(obj.name)
            # If we don't have a sample for the object, move on
            if not obj.sample:
                continue
            sid = obj.sample
            # If the sample ID isn't in our sample map, move on
            if sid not in sam_name2obj:
                continue
            kbase_sid = sam_name2obj[sid]['id']
            sample_ver = sam_name2obj[sid]['version']
            if obj.name not in obj_name2upa:
                continue
            kbase_sample = self.ss.get_sample({'id': kbase_sid})
            links = self.ss.get_data_links_from_sample({'id': kbase_sid,
                          'version': sample_ver})['links']
            link = {
                'upa': None,
                'id': kbase_sid,
                'version': sample_ver,
                'node': kbase_sample['node_tree'][0]['id'],
                'update': 1
               }
            # We could have multiple UPAs. So let's account for that.
            objs = [obj.name]
            if obj.type in linked_types:
                for ext in linked_types[obj.type]['exts']:
                     objs.append('%s%s' % (obj.name, ext))
            for oname in objs:
                 upa = obj_name2upa[oname]
                 if self.is_linked(upa, links):
                     continue
                 print("Linking %s to %s" % (upa, kbase_sid))
                 link['upa'] = upa
                 if self.dryrun:
                     continue
                 self.ss.create_data_link(link)

    def is_linked(self, upa, links):
        for link in links:
             if link['upa'] == upa:
                 _debug("%s is already linked to %s" % (upa, link['id']))
                 return True
        return False

    def generate_markdown_header(self):
        """
        Generate the header markdown block for the narrative.
        """
        data = self.study.__dict__
        _TEMPLATE = open("template.md").read()

        text = _TEMPLATE.format(**data)

        return text

    def _add_citation(self):
        metadata = self.ws.get_workspace_info({"id": self.study.wsid})[8]
        metadata['data_citation_URL'] = self.study.ds_url
        metadata['data_citation_DOI'] = self.study.ds_doi
        metadata['data_citation_title'] = self.study.ds_title
        self.ws.alter_workspace_metadata({"wsi": {"id": self.study.wsid}, 'new': metadata})

    def initialize_narrative(self):
        """
        Initialize the narrative.  If the study hasn't yet been
        initialized this will create the workspace and initalize
        the narrative with a markdown header.

        If the workspace already exists, this will populate the
        workspace name, workspace ID, Narrative ID, and narrative
        ref.
        """

        ws_name = "%s:%s" % (self.user, self.study.wsname)
        try:
            resp = self.ws.get_workspace_info({"workspace": ws_name})
            self.study.wsid = resp[0]
            self.study.workspace_name = resp[1]
            self.study.narrative_id = resp[8]['narrative']
            self.study.narrative_ref = "%s/%s" % (self.study.wsid, self.study.narrative_id)
            print("Previously Initialized %d" % (self.study.wsid))
            return
        except:
            print("Add")
        markdown = self.generate_markdown_header()
        resp = self.ws.create_workspace({"workspace": ws_name})
        self.study.wsid = resp[0]
        self.study.workspace_name = resp[1]
        meta = {
            "narrative": "1",
            "narrative_nice_name": "%s" % (self.study.name),
            "cell_count": 1,
            "searchtags": "narrative",
            "is_temporary": "false"
        }
        req = {"wsi": {"id": self.study.wsid}, "new": meta}
        self.ws.alter_workspace_metadata(req)
        narrative = json.load(open('narrative.json'))
        c0 = narrative['cells'][0]
        c0["source"] = markdown
        c0["metadata"]["kbase"]["attributes"]["title"] = self.study.name
        narrative["metadata"]["ws_name"] = ws_name
        narrative["metadata"]["name"] = self.study.name
        narrative["metadata"]["kbase"]["ws_name"] = ws_name
        narrative["metadata"]["creator"] = self.user
        narrative["metadata"]["kbase"]["creator"] = self.user
        usermeta = {
            "creator": self.user,
            "data_dependencies": "[]",
            "jupyter.markdown": "1",
            "is_temporary": "false",
            "job_info": "{\"queue_time\": 0, " +
                        "\"run_time\": 0, \"running\": 0, " +
                        "\"completed\": 0, \"error\": 0}",
            "format": "ipynb",
            "name": "%s" % (self.study.name),
            "description": "",
            "type": "KBaseNarrative.Narrative",
            "ws_name": ws_name
        }
        obj = {
            "name": "narrative",
            "type": "KBaseNarrative.Narrative-4.0",
            "data": narrative,
            "meta": usermeta
            }
        resp = self.ws.save_objects({"id": self.study.wsid, "objects": [obj]})
        self._add_citation()
        return resp


if __name__ == "__main__":
    c = Concierge(None)
