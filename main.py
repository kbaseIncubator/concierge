#!python

import sys
from gsheet_provider import GsheetProvider
from concierge import Concierge
from globus_transfer import GlobusTransfer
from yaml import load, Loader


if __name__ == "__main__":
    config = load(open("config.yaml").read(), Loader=Loader)
    if config['Provider'] == 'Gsheets':
        provider = GsheetProvider(config['Sheets']['TokenFile'])
    else:
        sys.write.stderr("Provider not supported.")
        sys.exit(1)
    if config['Transfer'] == 'Globus':
        trans = GlobusTransfer(config['User'], config['Globus'])
    else:
        sys.write.stderr("Transfer method not supported.")
    study = provider.get_study(config['Sheets'])
    print(study.name)
    conc = Concierge(study, transfer=trans)
    conc.sync()

