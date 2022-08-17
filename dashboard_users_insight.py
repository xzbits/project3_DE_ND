import psycopg2
from matplotlib import pyplot as plt
import configparser


config = configparser.ConfigParser()
config.read('dwh.cfg')
connect = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cursor = connect.cursor()

try:
    cursor.execute("SELECT COUNT(level) FROM users WHERE level = 'free'")

except psycopg2.Error as e:
    print("Error: select *")
    print(e)
no_free_users = cursor.fetchone()[0]

# Get number of paid users
try:
    cursor.execute("SELECT COUNT(level) FROM users WHERE level = 'paid'")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)
no_paid_users = cursor.fetchone()[0]

# Get number of Male users
try:
    cursor.execute("SELECT COUNT(*) FROM users WHERE gender = 'M'")

except psycopg2.Error as e:
    print("Error: select *")
    print(e)
no_male_users = cursor.fetchone()[0]

# Get number of Female users
try:
    cursor.execute("SELECT COUNT(*) FROM users WHERE gender = 'F'")

except psycopg2.Error as e:
    print("Error: select *")
    print(e)
no_female_users = cursor.fetchone()[0]

connect.close()

# Plotting
user_level = ["Free", "Paid"]
user_gender = ["Male", "Female"]
data_level = [no_free_users, no_paid_users]
data_gender = [no_male_users, no_female_users]
plot_data_level = data_level
plot_data_gender = data_gender
print(data_level)
print(data_gender)

# Creating plot
fig = plt.figure(figsize=(10, 10))
ax = fig.add_axes([0, 0, 1, 1])
fig1 = plt.figure(figsize=(10, 10))
ax1 = fig1.add_axes([0, 0, 1, 1])

ax.pie(plot_data_level,
       labels=user_level,
       autopct='%1.1f%%')
ax1.pie(plot_data_gender,
        labels=user_gender,
        autopct='%1.1f%%')
ax.set_title("Users Level", fontsize=20)
ax1.set_title("Users Gender", fontsize=20)

# show plot
plt.show()
