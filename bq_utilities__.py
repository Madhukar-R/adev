from sys import argv, exit
from google.cloud import bigquery
import datetime

PROJECT_ID = 'gabo-anonym'
TABLE_CSV = ''


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

    query = ("""select
                    sum(size_bytes)/pow(10,9) as size
                from
                    `{0}.{1}.__TABLES__`
                where
                    table_id between '{2}' and '{3}'
                    
                    """.format(project_name, ds_name, append_date_to_table(table_name, frompartiton), append_date_to_table(table_name, toparition)))
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


if __name__ == '__main__':
    if len(argv) < 3:
        print("Usage {} [Project ID] [fromfile?] [Num of partition days]".format(argv[0]))
        exit(1)

    project_id = argv[1]
    file_name = argv[2]
    num_days = argv[3]

    if file_name:
        get_bq_table_sizes_file(project_id, file_name, num_days)
    else:
        get_bq_table_sizes(project_id)