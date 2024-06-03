import getopt
from faker import Faker
import random
import sys

import numpy as np
import pandas as pd

input = sys.stdin.readline


def main():
    # Create external table as default
    table_type = "EXTERNAL"
    table_name = str()
    num_columns = 0
    num_partitions = 0
    file_extension = str()
    file_format = "text"
    rows = 0
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            " t:n:c:p:f:e:r:",
            [
                "table_type=",
                "table_name=",
                "num_columns=",
                "num_partitions=",
                "file_extension=",
                "file_format=",
                "rows="
            ],
        )
    except getopt.GetoptError:
        print(
            "python main.py -t <table type> -n <table_name> -c <num_columns>"
            + "-p <num_partitions> -e <file_extension> -f <file_format>"
            + "-r <rows>"
        )
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print(
                "python main.py -t <table type> -n <table_name> -c"
                + "<num_columns> -p <num_partitions> -e <file_extension>"
                + "-f <file_format> -r <rows>"
            )
            sys.exit()
        elif opt in ("-t", "--table_type"):
            table_type = arg.upper()
        elif opt in ("-n", "--table_name"):
            table_name = arg
        elif opt in ("-c", "--num_columns"):
            num_columns = int(arg)
        elif opt in ("-p", "--num_partitions"):
            num_partitions = int(arg)
        elif opt in ("-e", "--file_extension"):
            file_extension = arg.upper()
        elif opt in ("-f", "--file_format"):
            file_format = arg.upper()
        elif opt in ("-r", "--rows"):
            rows = int(arg)

    # print("table_type", table_type)
    # print("table_name", table_name)
    # print("num_columns", num_columns)
    # print("num_partitions", num_partitions)
    # print("file_extension", file_extension)
    # print("file_format", file_format)
    # print("rows", rows)

    col_list = gen_col(num_columns)
    part_list = gen_part(num_columns, num_partitions)

    output = "CREATE {0} TABLE {1} ({2}) PARTITIONED BY ({3}){4} STORED AS {5};".format(
        table_type,
        table_name,
        col_list,
        part_list,
        " STORED BY {}".format(file_extension) if file_extension else "",
        file_format,
    )
    print(output)

    all_col = list(col_list + ", " + part_list)
    total_col = num_columns + num_partitions

    gen_data(total_col, all_col, rows)


def gen_col(n):
    datatype = ["INT", "STRING", "BOOLEAN", "FLOAT"]
    precision = random.randint(4, 9)
    scale = random.randint(1, 4)
    datatype.append("DECIMAL({},{})".format(precision, scale))

    col_list = str()
    for i in range(1, n + 1):
        col_list += "col_" + str(i) + " " + random.choice(datatype)
        if i < n:
            col_list += ", "
    return col_list


def gen_part(n, m):
    part_list = str()
    if m <= 0:
        return part_list

    datatype = ["DATE"]
    for i in range(1, m + 1):
        part_list += "col_" + str(n + i) + " " + random.choice(datatype)
        if i < m:
            part_list += ", "
    return part_list


def gen_data(n, col, rows):
    sys.stdout = open('data.txt', 'w')

    col = "".join(col).split()
    fake = Faker()

    for i in range(rows):
        row = str()
        for j in range(1, 2 * n + 1, 2):
            dtype = col[j].strip(",")

            if dtype == "INT":
                row += str(random.randint(0, 10000))

            elif dtype == "STRING":
                row += '"{}"'.format(str(fake.name()))

            elif dtype == "FLOAT":
                row += str(fake.pyfloat(left_digits=2, right_digits=2))

            elif "DECIMAL" in dtype:
                p, s = dtype.split(",")
                p = int(p[-1])
                s = int(s[0])
                row += str(fake.pydecimal(left_digits=p - s, right_digits=s))

            elif dtype == "DATE":
                L = (
                    pd.date_range(start="2024-05-01", end="2024-05-31")
                    .strftime("%Y-%m-%d")
                    .tolist()
                )
                row += random.choice(L)

            elif dtype == "BOOLEAN":
                row += str(fake.pybool())
            row += ", "

        print(row.strip(", "))


if __name__ == "__main__":
    main()
