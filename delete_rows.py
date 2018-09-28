import csv

def rm_header(org_filename, final_filename)
with open(org_filename, 'r', encoding='utf8', newline='') as data_csv:
    data = csv.reader(data_csv)
    data_list=[]
    for line in data:
        data_list.append(line)

with open(final_filename, 'w', encoding='utf8', newline='') as csv_file:
    writer = csv.writer(csv_file)
    for item in data_list[11:-1]:
        writer.writerow(item)


