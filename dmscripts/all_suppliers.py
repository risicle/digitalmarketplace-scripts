import time

import sys
if sys.version_info > (3, 0):
    import csv
else:
    import unicodecsv as csv


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
    writer = csv.writer(
        output,
        delimiter=',',
        quotechar='"'
        # fieldnames=['Supplier ID', 'Supplier Name']
    )
    # writer.writeheader()
    suppliers = find_suppliers(data_api_client)

    for supplier in suppliers:
        count = progress(count, start_time)

        row = [
            supplier['id'],
            supplier['name']
        ]
        writer.writerow(row)
