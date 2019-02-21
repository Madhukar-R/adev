from sys import argv, exit
from google.cloud import bigquery
import datetime
import csv
import dns.resolver
import sys
import json
import argparse
import logging
import requests
try:
    import xmlrpclib
except ImportError:
    import xmlrpc.client as xmlrpclib  # for py3

PROJECT_ID = 'spotify-legal-hold'
TABLE_CSV = ''
retainer_api_endpoint = '_spotify-retainer-service._http.services.gew1.spotify.net'


def get_dates(numdays):
    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=numdays), base]
    return format_date_list(date_list)


def get_date_range(numdays):
    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]
    return format_date_list(date_list)


def format_date_list(date_list):
    formatted_date_tange = []
    for date in date_list:
        formatted_date_tange.append(format_date(date))
    return formatted_date_tange


def format_date(date):
    return date.strftime("%Y%m%d")


def append_date_to_table(table_name, date):
    return "{0}{1}".format(table_name, date)


def get_query(project_name, ds_name, table_name, frompartiton, toparition):

    query = ('''select
                    sum(size_bytes)/pow(10,9) as size
                from
                    `{0}.{1}.__TABLES__`
                where
                    table_id between '{2}' and '{3}'
                    
                    '''.format(project_name, ds_name, append_date_to_table(table_name, frompartiton), append_date_to_table(table_name, toparition)))
    return query


def get_bq_table_sizes(project_id):
    client = bigquery.Client(project=project_id)
    datasets = list(client.list_datasets())
    project = client.project

    if datasets:
        print('Datasets in project {}:'.format(project))
        for dataset in datasets:  # API request(s)
            print('\t{}'.format(dataset.dataset_id))
            tables = list(client.list_tables(client.dataset(dataset.dataset_id)))  # API request(s)
            if tables:
                for table in tables:
                    print('\t{0},{1}'.format(dataset.dataset_id, table.table_id))
                    break
            else:
                print('\tThis {} does not contain any tables.'.format(dataset.dataset_id))
    else:
        print('{} project does not contain any datasets.'.format(project))
    return "success"


def get_bq_table_sizes_file(project_id, file_name, num_days):
    client = bigquery.Client(project="spotify-user-extraction")

    num_partitions = 90

    if num_days:
        num_partitions = int(num_days)

    partition_range_dates = get_dates(num_partitions)

    total_size = 0.000000
    file_name = '../Input/tables.csv'

    print 'start'
    print "------------------------Individual table sizes in GBs----------------------------------"
    with open(file_name, "r") as infile:
        for filerow in infile:
            cols = filerow.split(",")
            #for date in date_range:
            query = get_query(cols[0].strip(), cols[1].strip(), cols[2].strip(), partition_range_dates[0], partition_range_dates[1])
            #print query
            query_job = client.query(query)  # API request
            rows = query_job.result()  # Waits for query to finish
            for queryrow in rows:
                total_size = total_size+queryrow.size
                print("table size {0}.{1}.{2} of partition range {3} and {4} is:, {5}, Average size:,{6}".format(cols[0].strip(), cols[1].strip(), cols[2].strip(), partition_range_dates[0], partition_range_dates[1], queryrow.size, queryrow.size/num_partitions))
            #break
    print "------------------------Individual table sizes----------------------------------"

    print "------------------------Summary--------------------------------------------------"

    print ("Total table size in GB:, {}, in TB:, {}, in PB:, {}".format(total_size, total_size/1000, total_size/1000000))

    print "------------------------Summary--------------------------------------------------"

    print "end"
    return "success"


def get_bq_table_expiration_dates(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    print "start"

    with open('{0}_project_tables.csv'.format(project_id), mode='w') as project_tables:
        project_tables_writer = csv.writer(project_tables, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for dataset in client.list_datasets():
            project_tables_writer.writerow(['', '', '', ''])
            dataset_ref = client.dataset(dataset.dataset_id)
            project_tables_writer.writerow(['', dataset.dataset_id, '', ''])
            print dataset.dataset_id
            for table in client.list_tables(dataset_ref):
                table_ref = dataset_ref.table(table.table_id)
                table_client = client.get_table(table_ref)
                # print ("{0},{1},{2}.{3},{4}".format(dataset.dataset_id, table_ref.table_id, dataset.dataset_id, table.table_id, table_client.expires))
                #if "HOURLY" not in table.table_id:
                project_tables_writer.writerow([project_id, dataset.dataset_id, table_ref.table_id, '{0}.{1}'.format(dataset.dataset_id,table.table_id), table_client.expires])

    print "end"

    return "success"


def get_api_client(endpoint):
    record = dns.resolver.query(endpoint, "SRV")[0]
    uri = "http://%s:%d/v1/retention" % (record.target, record.port)
    return xmlrpclib.ServerProxy(uri=uri, allow_none=True)


def setup_arg_parser():
    parser = argparse.ArgumentParser(
        description="Calls payment api to unlock subscription intervals",
        fromfile_prefix_chars="@")
    parser.add_argument(
        "api_call",
        help="The api to be called (e.g. account_v3.get_info_for_subject_access_request)",
    )
    parser.add_argument(
        "-a",
        "--retainer-api",
        default="_spotify-retainer-service._http.services.gew1.spotify.net",
        help="retainer api endpoint")
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity",
        action="store_true")
    parser.add_argument(
        "-p",
        "--pretty",
        help="pretty print",
        action="store_true")
    return parser.parse_args()


def setup_logging(verbose):
    if verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)


def multiple(fn, file_desc, pretty=False):
    for line in file_desc:
        args = line.strip().replace(" ", "\t").split("\t")
        ret = fn(*args)
        # sar = doSAR(api, *args)
        ident = 2 if pretty else None
        sys.stdout.write(json.dumps(ret, sort_keys=True, indent=ident, default=lambda x: str(x)))


def get_call(args):
    api = get_api_client(args.retainer_api)
    fn = api
    for c in args.api_call.split("."):
        fn = getattr(fn, c)  # feels like magic
    return fn


def get_system_z_expiration_dates(projectId, pretty=False):
    api = get_api_client(retainer_api_endpoint)

    record = dns.resolver.query(retainer_api_endpoint, "SRV")[0]
    URL = "http://%s:%d/v1/retention" % (record.target, record.port)

    with open('{0}_tables_retention'.format(projectId), mode='w') as table_retention_file:
        table_retention_file_writer = csv.writer(table_retention_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        with open('{0}_project_tables.csv'.format(projectId)) as project_tables_file:
            project_tables_file_reader = csv.reader(project_tables_file, delimiter=',')
            line_count = 0
            for row in project_tables_file_reader:
                params = {'type': "BIGQUERY", 'projectId': row[0], 'datasetId': row[1], 'tableId': row[2]}
                r = requests.get(url=URL, params=params)
                jsondata = r.json()
                print jsondata['data']['retention']
                table_retention_file_writer.writerow([row[0], row[1], row[2], jsondata['data']['retention']])

                line_count += 1

    return "success"


def print_table_name():
    get_bq_table_expiration_dates(PROJECT_ID, "", "")
    return "success"


def print_system_z_retention():

    # args = setup_arg_parser()
    # setup_logging(args.verbose)
    # multiple(get_call(args), sys.stdin, args.pretty)

    get_system_z_expiration_dates(PROJECT_ID, True)

    return "success"


def retain_tables():
    get_bq_table_expiration_dates(PROJECT_ID, "", "")
    get_system_z_expiration_dates(PROJECT_ID, True)
    return "success"


if __name__ == '__main__':

    print_table_name()

    # print_system_z_retention()

    # if len(argv) < 3:
    #     print("Usage {} [Project ID] [fromfile?] [Num of partition days]".format(argv[0]))
    #     exit(1)
    #
    # project_id = argv[1]
    # file_name = argv[2]
    # num_days = argv[3]
    #
    # if file_name:
    #     get_bq_table_sizes_file(project_id, file_name, num_days)
    # else:
    #     get_bq_table_sizes(project_id)