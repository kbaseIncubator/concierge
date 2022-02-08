import json
import os
import sys
from yaml import load, Loader
import requests
from models import Study, Sample, File, DataObject

def jprint(obj):
    print(json.dumps(obj, indent=2))

class NMDCProvider():
    """
    NMDC DataProvider Class
    """
    _conf_fn = "nmdc.yaml"

    def __init__(self):
        """
        Init: Nothing to do at this time.
        """
        self.conf = load(open(self._conf_fn).read(), Loader=Loader)
        self._nmdc_url = self.conf['Global']['URL']
        self.mapping = self.conf['Global']['Mapping']

    def get_study(self, config):
        """
        This should fetch all the info for a study
        (e.g. study info, samples, and data)
        """
       
        self.user = config['User']
        self.mapping = config['Mapping']
        study_id = config['Study'] 
        study = self.get_study_info(study_id)
        study.samples_headers, study.samples, data = self.get_samples(study_id)
        study.data_objects = self.get_objects(data)
        return study

    def get_study_info(self, study_id):
        """
        Method to return study information
        """

        url = "%sapi/study/%s" % (self._nmdc_url, study_id)
        resp = requests.get(url)
        p = resp.json()
        ws_name = "%s" % (study_id.replace(":", "_"))
        ss_name = study_id.replace(":", "_")
        pub_doi = None
        if len(p["publication_dois"]) > 0:
            pub_doi = p["publication_dois"][0]
        url = "%s/details/study/%s" % (self._nmdc_url, study_id)
        pub = None
        if pub_doi:
            pub = p["publication_doi_info"][pub_doi]
        study = Study(p['name'],
                      study_id,
                      ws_name,
                      self.user,
                      p['principal_investigator_name'],
                      pub_doi,
                      url,
                      p['description'],
                      'kbase',
                      ss_name,
                      callback=self.study_update_callback)
        if pub:
            study.set_dataset(pub['DOI'],
                              pub['title'],
                              pub['URL'])
        return study

    def study_update_callback(self, study):
        """
        Future callback handler
        """
        return

    def fix_sample(self,sample):
        if sample["feature"] in self.conf["Global"]["Problematic_ENVO_Feature_Terms"]:
            sample["feature"] = None
        if sample["material"] in self.conf["Global"]["Problematic_ENVO_Material_Terms"]:
            sample["material"] = None

    def get_samples(self, study_id):
        """
        Method to construct samples.
        Queries NMDC API to get the list of samples for the study.
        Should return a list in a KBase template namespace
        """
        # TODO: Deal with more than 100 hits
        url = "%sapi/biosample/search?offset=0&limit=100" % (self._nmdc_url)
        q = {"conditions": [
                {"value": study_id,
                 "table": "study",
                 "op": "==",
                 "field": "study_id"
                 }],
             "data_object_filter": []
             }
        h = {
             "content-type": "application/json",
             "accept": "application/json"
             }
        resp = requests.post(url, headers=h, data=json.dumps(q))
        if resp.status_code != 200:
            raise ValueError("Failed to fetch samples")
        n_samples = resp.json()['results']
        samples = []
        data = []
        for raw_data in n_samples:
            sample_data = {}
            for k in self.mapping:
                nmdc_k = self.mapping[k]
                sample_data[k] = raw_data[nmdc_k]
            self.fix_sample(sample_data)
            samples.append(Sample(sample_data))
            data.append(raw_data["omics_processing"])
        headers = self.mapping.keys() 
        return headers, samples, data

    def _mk_metag(self, id, sid):
        name = "%s_metagenome" % (id.replace(":", "_"))
        dtype = 'metagenome'
        fasta_fn = '%s/assembly/%s_assembly_contigs.fna' % (id, id)
        gff_fn = '%s/annotation/%s_functional_annotation.gff' % (id, id)
        params = {
                  'Assembly': fasta_fn,
                  'GFF': gff_fn
                 }
        return DataObject(name, dtype, params, sid)

    def get_objects(self, data):
        objs = []
        # iterate over all samples 
        for sample_row in data:
            for data_row in sample_row:
                if data_row['annotations']['omics_type'] != 'Metagenome':
                    continue
                sid = data_row["biosample_id"]
                id = data_row['id']
                # If old ID we need to extract the informed by
                if id.startswith('gold:'):
                    id = data_row['omics_data'][0]['name'].split(' ')[-1]
                mg_obj = self._mk_metag(id, sid)
                objs.append(mg_obj)
        return objs


if __name__ == "__main__":
    fn = "config_nmdc.yaml"
    config = load(open(fn).read(), Loader=Loader)

    np = NMDCProvider()
    np.get_study(config['Provider'])
#    d = json.load(open('s.json'))
#    np.get_objects(d)
#    print(np.study.name) 
