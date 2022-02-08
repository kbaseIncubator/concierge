#!python

import sys
from gsheet_provider import GsheetProvider
from nmdc_provider import NMDCProvider
from concierge import Concierge
from globus_transfer import GlobusTransfer
from yaml import load, Loader


if __name__ == "__main__":
    fn = "config.yaml"
    if len(sys.argv) > 1:
        fn = sys.argv[1]
    config = load(open(fn).read(), Loader=Loader)
    token = None
    if 'TokenFile' in config:
        print("Reading token: %s" % (config['TokenFile']))
        with open(config['TokenFile']) as f:
            token = f.read().rstrip()
    if config['Provider'] == 'Gsheets':
        provider = GsheetProvider(config['Sheets']['TokenFile'])
        param = config['Sheets']
    elif config['Provider'] == 'NMDC':
        provider = NMDCProvider()
        param = config['NMDC'] 
    else:
        sys.stderr.write("Provider not supported.\n")
        sys.exit(1)
    if config['Transfer'] == 'Globus':
        trans = GlobusTransfer(config['User'], config['Globus'])
    else:
        sys.write.stderr("Transfer method not supported.")
    study = provider.get_study(param)
    print(study.name)
    conc = Concierge(study, transfer=trans, token=token, statefile=config.get("StateFile"))
    conc.sync()

