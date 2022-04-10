from random import randrange
from datetime import timedelta
from datetime import datetime
import random
import re

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

# list of codes
warnings = ['0', '1', '2', '3', '4', '5', '6', '7']

date_format = '%Y-%m-%d %H:%M:%S'
start_date = "2022-03-01 00:00:00"
error_chance = 3

days = input('How much days report should include (200 max)? \n')
# 200 max not to mess with February days :)

if int(days) > 200:
    print('Wrong number')
    exit(1)
if int(days) < 1:
    print('Wrong number')
    exit(1)
if int(days) == None:
    print('REQUIRED')

entries_per_day = input('How much records per day? \n')
if int(entries_per_day) == None:
    print("Required")

start_datetime = datetime.strptime(start_date, date_format)
start_datetime_end = start_datetime.replace(hour=23, minute=59, second=59)
end_datetime = start_datetime + timedelta(days=days)
how_much_days  = (end_datetime.date() - start_datetime.date()).days

f = open("input_file", 'wb')
for i in range(how_much_days):
    for j in range(int(entries_per_day)):
        if random.randrange(0, 100, 1) > error_chance:
            warning_code = str(random.choice(warnings))
            timestamp = str(random_date(start_datetime, start_datetime_end))
            result_str = timestamp + ',' + warning_code + '\n'
            f.write(str.encode(result_str))
        else:
            f.write(b'ERROR\n')
            f.write(b'01/01/err2022 03\n')
    #next day
    if start_datetime.day > 29:
        start_datetime = start_datetime.replace(day=1, hour=0, minute=0, second=0, month=start_datetime.month+1)
        start_datetime_end = start_datetime_end.replace(day=start_datetime.day, month=start_datetime.month)
    else:
         start_datetime = start_datetime.replace(day=start_datetime.day + 1, hour=0, minute=0, second=0)
         start_datetime_end = start_datetime_end.replace(day=start_datetime.day, month=start_datetime.month)
f.close()
