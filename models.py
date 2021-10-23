from yaml import load, Loader

class Study:
    """
    Study Object
    """
    def __init__(self, name, id, wsname, user, pi, doi, url, 
                 description, tmpl, sample_set, callback=None):
        self.name = name
        self.id = id
        self.wsname = wsname
        self.pi = pi
        self.doi = doi
        self.url = url
        self.description = description
        self.tmpl = tmpl
        self.sampleset = sample_set
        # DS
        self.ds_doi = None
        self.ds_title = None
        self.ds_url = None
        self.update_callback = callback

        #
        self.data_objects = []
        self.samples = None
        self.samples_headers = None
        self.sample_count = 0
        self.pubs = ""

        # Handled by Concierge
        self.wsid = None
        self.workspace_name = None
        self.narrative_id = None
        self.narrative_ref = None


    def set_dataset(self, ds_doi, ds_title, ds_url):
        self.ds_doi = ds_doi
        self.ds_title = ds_title
        self.ds_url = ds_url
        self.dataset = "[%s](%s)" % (self.ds_title, self.ds_url)
         
    def set_pubs(self, pubs):
        # TODO
        self.pubs = []


class File:
    """
    File Object: This captures important information about an
    NMDC file.

    Variables:
    fn: File name
    src: Source path (relative to base)
    url: data url
    """

    #def __init__(self, fn, access_method, url=None, src=None, endpt=None, clientid=None):
    def __init__(self, fn):
        self.fn = fn
#        self.access_method = access_method
#        # Used for Globus
##        self.src = src
#        self.src_endpt = endpt
#        self.clientid = clientid
#
#        # Used for URL staging
#        self.url = url
        self.update_callback = None

class DataObject:
    """
    Object to hold workspace object information.
    """
    config = load(open("concierge.yaml").read(), Loader=Loader)

    def __init__(self, name, type, params, sample=None):  
        self.name = name
        self.type = type
        self.sample = sample
        self.conf = self.config['Imports'][self.type]
        self.params = {}
        self.params[self.conf['name']] = name
        self.method = self.conf['Method']
        self.appid = self.conf["AppId"]
        for k, v in self.conf['default_params'].items():
            params[k] = v
        self.files = []
        self._process_files(params)
        self.update_callback = None
        # TODO: Something about versioning

    def _process_files(self, params):
        fts = self.conf['Files'] 
        for ft, ftp in fts.items():
            fp = params[ft]
            fn = fp.split('/')[-1]
#            am = params['Access Method']
#            endpt = params['SourceEndpoint']
#            clientid = params['ClientId']
#            fo = File(fn, am, src=fp, endpt=endpt)
            fo = File(fn)
            self.files.append(fo)
            self.params[ftp['param_name']] = fn


class Sample:
    """
    Sample Object to hold workspace object information.
    """
    def __init__(self, data):
        self.update_callback = None
        self.data = data
        self.name = data['name']
        self.kbaseid = None


