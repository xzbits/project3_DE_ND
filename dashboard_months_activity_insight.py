import psycopg2
import configparser
from matplotlib import pyplot as plt
import numpy as np


config = configparser.ConfigParser()
config.read('dwh.cfg')
connect = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cursor = connect.cursor()


def fulfill_date(data):
    date_in_data = [x[0] for x in data]
    for i in range(1, 31):
        if i not in date_in_data:
            data.append((i, 0))


# Get visits from Male user by day in November
try:
    cursor.execute(
        "SELECT time.day, COUNT(*) FROM (songplays JOIN time ON songplays.start_time = time.start_time) "
        "JOIN users ON songplays.user_id = users.user_id WHERE users.gender = 'M' GROUP BY time.day;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

visits_male = cursor.fetchall()
fulfill_date(visits_male)
visits_male.sort(key=lambda x: x[0])

# Get visits from Female user by day in November
try:
    cursor.execute(
        "SELECT time.day, COUNT(*) FROM (songplays JOIN time ON songplays.start_time = time.start_time) "
        "JOIN users ON songplays.user_id = users.user_id WHERE users.gender = 'F' GROUP BY time.day;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

visits_female = cursor.fetchall()
fulfill_date(visits_female)
visits_female.sort(key=lambda x: x[0])


# Get visits from free user by day in November
try:
    cursor.execute(
        "SELECT time.day, COUNT(*) FROM (songplays JOIN time ON songplays.start_time = time.start_time) "
        "JOIN users ON songplays.user_id = users.user_id WHERE users.level = 'free' GROUP BY time.day;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

visits_free = cursor.fetchall()
fulfill_date(visits_free)
visits_free.sort(key=lambda x: x[0])

# Get visits from paid user by day in November
try:
    cursor.execute(
        "SELECT time.day, COUNT(*) FROM (songplays JOIN time ON songplays.start_time = time.start_time) "
        "JOIN users ON songplays.user_id = users.user_id WHERE users.level = 'paid' GROUP BY time.day;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

visits_paid = cursor.fetchall()
fulfill_date(visits_paid)
visits_paid.sort(key=lambda x: x[0])

connect.close()

# Get gender data
date = [x[0] for x in visits_female]
no_male_visits = [x[1] for x in visits_male]
no_female_visits = [x[1] for x in visits_female]

# Get level data
no_free_visits = [x[1] for x in visits_free]
no_paid_visits = [x[1] for x in visits_paid]

fig = plt.figure()
fig1 = plt.figure()
ax = fig.add_axes([0, 0, 1, 1])
ax1 = fig1.add_axes([0, 0, 1, 1])

# Plotting number of visits by date and gender
ax.bar(date, no_male_visits, color='b')
ax.bar(date, no_female_visits, bottom=no_male_visits, color='r')
ax.set_title('Number of visits by date and gender')
ax.set_xlabel('Date')
ax.set_ylabel('Number of visits')
ax.set_xticks(np.arange(1, 31, 1))
ax.set_yticks(np.arange(0, 511, 30))
ax.legend(labels=['Male', 'Female'])

# Plotting number of visits by data and level
ax1.bar(date, no_free_visits, color='b')
ax1.bar(date, no_paid_visits, bottom=no_free_visits, color='r')
ax1.set_title('Number of visits by date and level')
ax1.set_xlabel('Date')
ax1.set_ylabel('Number of visits')
ax1.set_xticks(np.arange(1, 31, 1))
ax1.set_yticks(np.arange(0, 511, 30))
ax1.legend(labels=['Free', 'Paid'])

plt.show()

connect.close()
