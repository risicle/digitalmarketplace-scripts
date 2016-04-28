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


def find_services(data_api_client):
    for service in data_api_client.find_services_iter():
        yield service


def progress(count, start_time):
    if count % 100 == 0:
        print("{} services in {}s".format(count, time.time() - start_time))
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

    services = find_services(data_api_client)

    for service in services:

        count = progress(count, start_time)

        if service['frameworkSlug'] in ['g-cloud-6', 'g-cloud-7']:

            row = [
                service['id'],
                service['serviceName'],
                service['supplierId'],
                service['supplierName'],
                service['frameworkName'],
                service['serviceSummary'].replace('\n', ' ').replace('\r', ''),
                ', '.join(service['serviceBenefits']) if service['serviceBenefits'] else '',
                ', '.join(service['serviceFeatures']) if service['serviceFeatures'] else ''
            ]
            writer.writerow(row)

