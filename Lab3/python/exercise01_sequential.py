from collections import defaultdict

def exercise01_seq():
    with open('../data/temperature-readings-small.csv', 'rb') as infile:
        year_temp = defaultdict(list)
        for line in infile:
            values = line.split(";")
            year = int(values[1][:4])
            temp = float(values[3])

            year_temp[year].append(temp)

        for year in year_temp:
            print(year, min(year_temp[year]), max(year_temp[year]))

exercise01_seq()
