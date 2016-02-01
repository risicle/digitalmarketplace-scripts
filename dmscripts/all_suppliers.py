import time

import sys
if sys.version_info > (3, 0):
    import csv
else:
    import unicodecsv as csv

from dmutils.apiclient import HTTPError


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
    suppliers_without_services = 0
    writer = csv.writer(
        output,
        delimiter=',',
        quotechar='"'
    )

    suppliers = find_suppliers(data_api_client)

    for supplier in suppliers:
        count = progress(count, start_time)

        try:
            # TODO: this is hugely inefficient. Use thread pooling if we're going to run this again
            services = data_api_client.find_services(supplier['id'])

            if services['services']:
                for service in services['services']:

                    row = [
                        supplier['id'],
                        supplier['name'],
                        supplier.get('dunsNumber', ''),
                        '@{}'.format(supplier['contactInformation'][0].get('email').split('@', 1)[1]),
                        supplier['contactInformation'][0].get('postcode'),
                        supplier['contactInformation'][0].get('country'),
                        '"{}"'.format(service['id']),
                        service['serviceName'],
                        service['lot'],
                        service['frameworkSlug']
                    ]
                    writer.writerow(row)

            else:
                suppliers_without_services += 1

        except HTTPError:
            print('Error getting services for supplier {}'.format(supplier['id']))
            pass

    print('Suppliers without services: {}'.format(suppliers_without_services))
