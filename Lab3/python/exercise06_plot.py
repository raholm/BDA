import string
import matplotlib.pyplot as plt

from datetime import datetime

def exercise06_plot():
    chars_remove = set(["(", ")", " ", "u", "'"])

    avg_year_month = []
    avg_temp = []

    with open("../nsc_result/6/part-00000", "r") as file:
        for line in file:
            elements = ''.join([char for char in line if char not in chars_remove]).split(",")
            year_month, temp = datetime.strptime(elements[0].strip(), "%Y-%m"), float(elements[1])

            avg_year_month.append(year_month)
            avg_temp.append(temp)

    plt.plot(avg_year_month, avg_temp)
    plt.xlabel("Date", size=15)
    plt.ylabel("Temperature", size=15)
    plt.show()

def main():
    plt.style.use('ggplot')
    exercise06_plot()

if __name__ == "__main__":
    main()
