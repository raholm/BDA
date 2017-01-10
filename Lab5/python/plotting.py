import matplotlib.pyplot as plt

from datetime import datetime

def plot_estimated_temps():

    date_temp = [('1900-01-01 04-00-00', 4.0395471934721945),
                 ('1900-01-01 06-00-00', 4.207136150573667),
                 ('1900-01-01 08-00-00', 5.099398734070549),
                 ('1900-01-01 10-00-00', 6.053440665846871),
                 ('1900-01-01 12-00-00', 6.510899151286655),
                 ('1900-01-01 14-00-00', 6.508231735020072),
                 ('1900-01-01 16-00-00', 6.052150465992286),
                 ('1900-01-01 18-00-00', 5.440059673300056),
                 ('1900-01-01 20-00-00', 5.0612247041114875),
                 ('1900-01-01 22-00-00', 4.624930375219939),
                 ('1900-01-01 23-59-59', 4.233952353396673)]


    temps = [temp for (date, temp) in date_temp]
    dates = [datetime.strptime(date, "%Y-%m-%d %H-%M-%S")
             for (date, temp) in date_temp]

    plt.style.use('ggplot')
    plt.plot(dates, temps)
    plt.xlabel("Date", size=15)
    plt.ylabel("Temperature", size=15)
    plt.show()

def plot_date_kernel():
    dist = []
    kernel = []

    with open("../result/kernel_result/date_kernel/part-00000", "r") as infile:
        for line in infile:
            elements = line.strip().split(",")
            dist.append(float(elements[0][1:]))
            kernel.append(float(elements[1][1:-1]))

    plt.style.use('ggplot')
    plt.plot(dist, kernel, 'o', linewidth=4)
    plt.xlim([-1000, 30000])
    plt.ylim([-0.2, 1.2])
    plt.title("Date Kernel")
    plt.xlabel("Distance", size=15)
    plt.ylabel("Kernel Weight", size=15)
    plt.show()


def plot_time_kernel():
    dist = []
    kernel = []

    with open("../result/kernel_result/time_kernel/part-00000", "r") as infile:
        for line in infile:
            elements = line.strip().split(",")
            dist.append(float(elements[0][1:]))
            kernel.append(float(elements[1][1:-1]))

    plt.style.use('ggplot')
    plt.plot(dist, kernel, 'o', linewidth=4)
    plt.xlim([-1, 15])
    plt.ylim([-0.2, 1.2])
    plt.title("Time Kernel")
    plt.xlabel("Distance", size=15)
    plt.ylabel("Kernel Weight", size=15)
    plt.show()


def plot_distance_kernel():
    dist = []
    kernel = []

    with open("../result/kernel_result/distance_kernel/part-00000", "r") as infile:
        for line in infile:
            elements = line.strip().split(",")
            dist.append(float(elements[0][1:]))
            kernel.append(float(elements[1][1:-1]))

    plt.style.use('ggplot')
    plt.plot(dist, kernel, 'o', linewidth=4)
    plt.xlim([-100, 1300])
    plt.ylim([-0.2, 1.2])
    plt.title("Distance Kernel")
    plt.xlabel("Distance", size=15)
    plt.ylabel("Kernel Weight", size=15)
    plt.show()


plot_estimated_temps()
# plot_date_kernel()
# plot_time_kernel()
# plot_distance_kernel()


