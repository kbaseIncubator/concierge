import json
import os
import sys
from yaml import load, Loader
from models import Study, Sample, File, DataObject
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class GsheetProvider():
    """
    Google Sheets DataProvider Class
    """
#    config = load(open("gsheet.yaml").read(), Loader=Loader)

    def __init__(self, keyfile):
        """
        keyfile: Globus API Keyfile
        """
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive.file",
                 "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
        self.client = gspread.authorize(credentials)

    def get_study(self, config):
        workbook = config['Workbook']
        spreadsheet = self.client.open(workbook)

        study_sheet = config['Study']
        samples_sheet = config['Samples']
        data_sheets = config['Data']

        study = self.get_study_info(spreadsheet, study_sheet)
        study.samples_headers, study.samples = self.get_samples(spreadsheet, samples_sheet)
        study.data_objects = self.get_objects(spreadsheet, data_sheets)
        return study

    def get_study_info(self, spreadsheet, study_sheet):
        """
        Method to return study information
        """
        ws = spreadsheet.worksheet(study_sheet)
        data = ws.get()
        p = {}
        for row in data[1:]:
            p[row[0]] = row[1]

        study = Study(p['Study Name'],
                      p['Study ID'],
                      p['Workspace Name'],
                      p['User'],
                      p['Principle Investigator'],
                      p['Publication_DOI'],
                      p['URL'],
                      p['Description'],
                      p['Samples Template'],
                      p['Sample Set'],
                      callback=self.study_update_callback)

        study.set_dataset(p['Dataset_DOI'],
                          p['Dataset_Title'],
                          p['Dataset_URL'])
        return study

    def study_update_callback(self, study):
        """
        Future callback handler
        """
        return
     
    def get_samples(self, spreadsheet, samples_sheet):
        """
        Method to construct samples.
        Should return a list in a KBase template namespace
        """
        ws = spreadsheet.worksheet(samples_sheet)
        headers, g_samples = self._fetch_gsheet(spreadsheet, ws)
        # data = ws.get()
        # TODO: Allow mapping
        samples = []
        name = headers[0]
        for sample_data in g_samples:
            samples.append(Sample(sample_data, name_col=name))
        return headers, samples

#    def _add_transport(self, obj):
#        if self.config['Transfer']['Method'] == 'Globus':
#            obj['Access Method'] = self.config['Transfer']['Method']
#            # May change this
#            obj['ClientId'] = self.config['Globus']['ClientId']
#            obj['SourceEndpoint'] = self.config['Globus']['SourceEndpoint']

    def get_objects(self, spreadsheet, data_sheets):
        objs = []
        for dtype, sheet in data_sheets.items():
            ws = spreadsheet.worksheet(sheet)
            _, rows = self._fetch_gsheet(spreadsheet, ws)
            for obj in rows:
#                self._add_transport(obj)
                objs.append(DataObject(obj['Name'], dtype, obj, sample=obj.get('Sample')))
        return objs

    def get_formatting(self, spreadsheet, sheet):
        # TODO: Make it figure out the range automatically
        resp = spreadsheet.fetch_sheet_metadata({
                  'includeGridData': True,
                  'ranges': ["%s!A1:Z3000" % (sheet)],
                  'fields': 'sheets.data.rowData.values.effectiveFormat'
               })
        return resp['sheets'][0]['data'][0]['rowData']

    def _fetch_gsheet(self, spreadsheet, ws, resolve_uri=False):
        """
        Fetch a Google sheet and return it in a format
        similar to the CSV reader.
        """
        data = ws.get()
        format = self.get_formatting(spreadsheet, ws.title)
        map = {}
        headings = {}
        linkcol = {}
        for idx, header in enumerate(data[0]):
            map[header] = idx
            headings[idx] = header
            if header.lower().startswith("link"):
                linkcol[idx] = self.get_column(header)
        results = []
        rn = 1
        for row in data[1:]:
            newrow = {}
            # Init row since it could be short
            for h in map:
                newrow[h] = ""
            for idx, field in enumerate(row):
                newrow[headings[idx]] = field
                fmt = None
                if 'effectiveFormat' in format[rn]['values'][idx] and \
                   'textFormat' in format[rn]['values'][idx]['effectiveFormat']:
                    fmt = format[rn]['values'][idx]['effectiveFormat']['textFormat']
                if resolve_uri and fmt and 'link' in fmt and 'uri' in fmt['link']:
                    newrow[headings[idx]] = fmt['link']['uri']
            results.append(newrow)
            rn += 1
        return data[0], results


if __name__ == "__main__":
    gp = GsheetProvider()
    print(gp.study.name) 
