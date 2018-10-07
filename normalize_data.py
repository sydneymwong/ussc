import csv
import os
import string
import json

with open('data/headers.csv', 'r') as headers_file:
    header_reader = csv.reader(headers_file)
    HEADERS = list(next(header_reader))


with open('data/dictionary.json', 'r') as mappings_file:
    MAPPINGS = json.load(mappings_file)


def get_prefixes():
    return set([i.rstrip(string.digits) for i in HEADERS])


def get_array_dict():

    arrays = {}
    single_headers = []

    for header in HEADERS:
        header_stripped = header.rstrip(string.digits)
        header_suffix = header[len(header_stripped):]
        try:
            int(header_suffix)
        except ValueError:
            single_headers.append(header)
            continue
        if header_stripped not in arrays:
            arrays[header_stripped] = []
        arrays[header_stripped].append(int(header_suffix))

    for k in list(arrays):
        if arrays[k][0] is not 1:
            values = arrays[k]
            arrays.pop(k, None)
            for v in values:
                header = k + str(v)
                single_headers.append(header)

    return arrays, single_headers


ARRAY_HEADERS, SINGLE_HEADERS = get_array_dict()


def generate_csv_dicts():
    with open(os.path.join('data', 'data.csv'), 'r') as f:
        for line in f:
            row = line.rstrip('\n').split(',')
            yield {header: value for header, value in zip(HEADERS, row)}


def unflatten_row(row):
    flat = {}

    # iterate through variable stubs (represented as keys of array_headers)
    # and consolidate the cells corresponding to stubbed variables
    for k in ARRAY_HEADERS.keys():
        if k not in flat.keys():
            flat[k] = []
        # array_headers[k] is the list of variable suffixes (i.e., [1,2,3,4])
        for val in ARRAY_HEADERS[k]:
            # create the stub + suffix (i.e., DESCRIP2)
            curr_header = k + str(val)
            # take the value from the stub + suffix variable and append it to
            # the value in the new dictionary corresponding to stub
            flat[k].append(row[curr_header])

    # now iterate through variables that are independent and do not have stubs
    # (i.e., NEWRACE) and add them (as-is) to the new dictionary
    for single in SINGLE_HEADERS:
        flat[single] = row[single]
    # return the new dictionary
    return flat


def decode_row(row):
    for k, mapping in MAPPINGS.items():
        row[k] = mapping.get(row[k])


def unflatten_rows():
    return [decode_row(unflatten_row(row)) for row in generate_csv_dicts()]
