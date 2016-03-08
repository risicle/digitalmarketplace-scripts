import time

import sys
sys.path.insert(0, '.')
if sys.version_info > (3, 0):
    import csv
else:
    import unicodecsv as csv

from dmapiclient import HTTPError


def find_suppliers(data_api_client):
    for supplier in data_api_client.find_suppliers_iter():
        yield supplier


def progress(count, start_time):
    if count % 100 == 0:
        print("{} suppliers in {}s".format(count, time.time() - start_time))
    return count + 1


def list_suppliers(data_api_client, output):
    start_time = time.time()
    count = 0
    # suppliers_without_services = 0
    writer = csv.writer(
        output,
        delimiter=',',
        quotechar='"'
    )

    suppliers = find_suppliers(data_api_client)

    for supplier in suppliers:
        count = progress(count, start_time)

        supplier_framework = None
        try:
            supplier_framework = data_api_client.get_supplier_framework_info(
                supplier['id'], 'digital-outcomes-and-specialists')['frameworkInterest']
        except HTTPError:
            pass

        if supplier_framework and supplier_framework['onFramework']:
            row = [
             supplier['id'],
             supplier['contactInformation'][0].get('postcode') or '(NA)'
            ]
            writer.writerow(row)

