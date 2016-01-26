"""

Usage:
    scripts/all-suppliers.py
    <data_api_url> <data_api_token> [--filename=<filename>]
"""
import sys
sys.path.insert(0, '.')

from docopt import docopt
from dmutils.apiclient import DataAPIClient
from dmscripts.all_suppliers import list_suppliers


if __name__ == '__main__':
    arguments = docopt(__doc__)

    data_api_client = DataAPIClient(arguments['<data_api_url>'], arguments['<data_api_token>'])
    output = open(arguments.get('--filename'), 'w+') if arguments.get('--filename') else sys.stdout

    list_suppliers(data_api_client, output)
