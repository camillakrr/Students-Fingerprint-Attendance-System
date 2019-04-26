import random
import secrets
from datetime import datetime

import names
from pg4nosql.PostgresNoSQLClient import PostgresNoSQLClient

courses = ['Discrete Math and Logic', 'Data Structure and Algorithms', 'Analitical Geometry and Linear Algebra',
           'English', 'Introduction to Programming', 'Mathematical Analysis', 'Introduction to AI',
           'Data Modeling and Databases', 'Software Project', 'Probability and Statistics', 'Networks',
           'Control Theory', 'Digital Signal Processing', 'Information Retrieval', 'Data Mining',
           'Lean Software Development', 'Mechanics and Machines', 'Software System Design', 'Life Safety',
           'Practical Machine Learning and Deep Learning', 'Software Quality and Reliability']
types = ['lecture', 'tutorial', 'lab']
weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
time_slots = ['7:00', '8:40', '10:20', '12:00', '13:40', '15:20', '17:00', '18:40', '20:20', '22:00']


def create_students(db):
    print("Generating students...")
    students = db['students']
    for i in range(0, 8000):
        first_name = names.get_first_name()
        last_name = names.get_last_name()
        year = random.randint(1, 5)
        group = random.randint(1, 20)
        fingerprint_hash = secrets.token_hex(nbytes=16)
        students.put({'first_name': first_name, 'last_name': last_name, 'year': year, 'group': group,
                      'fingerprint_hash': fingerprint_hash})
    print("Students created")


def create_scanners(db):
    print("Generating scanners...")
    scanners = db['scanners']
    for i in range(100, 600):
        serial_number = random.randint(1000000000000, 9999999999999)
        scanners.put({'classroom': i, 'serial_number': serial_number})
    print("Scanners created")


def create_classes(db):
    print("Generating classes...")
    classes = db['classes']
    classrooms = dict()
    groups = dict()
    for i in range(10000):
        course = secrets.choice(courses)
        type = secrets.choice(types)
        classroom = random.randint(100, 600)
        year = random.randint(1, 5)
        group = random.randint(1, 20)
        weekday = secrets.choice(weekdays)
        time_slot = secrets.choice(time_slots).split(":")
        hours = time_slot[0]
        minutes = time_slot[1]
        if weekday + hours + minutes + str(classroom) not in classrooms:
            if type == "lab":
                if weekday + hours + minutes + str(year) + str(group) not in groups:
                    classrooms[weekday + hours + minutes + str(classroom)] = "true"
                    groups[weekday + hours + minutes + str(year) + str(group)] = "true"
                    classes.put({"course": course, "type": type, "classroom": classroom, "year": year,
                                 "groups": "{" + str(group) + "}", "weekday": weekday,
                                 "start_time": {"hours": hours, "minutes": minutes}})
            else:
                for j in range(20):
                    if weekday + hours + minutes + str(year) + str(j + 1) in groups:
                        break
                if j == 19:
                    classrooms[weekday + hours + minutes + str(classroom)] = "true"
                    for q in range(20):
                        groups[weekday + hours + minutes + str(year) + str(q + 1)] = "true"
                    classes.put({"course": course, "type": type, "classroom": classroom, "year": year,
                                 "groups": "{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}", "weekday": weekday,
                                 "start_time": {"hours": hours, "minutes": minutes}})
    print("Classes created")


def create_attendance(db):
    print("Generating attendance...")
    students = db['students']
    classes = db['classes']
    attendance = db['attendance']
    for i in range(10000):
        unixtime = random.randint(1483228800, 1577836800)
        date = datetime.fromtimestamp(unixtime).strftime('%Y-%m-%d %H:%M:%S')
        weekday = datetime.fromtimestamp(unixtime).strftime("%A")
        hours = int(datetime.fromtimestamp(unixtime).strftime("%H"))
        minutes = int(datetime.fromtimestamp(unixtime).strftime("%M"))
        ts = hours * 60 + minutes
        classes_by_time = classes.query(
            "json->>'weekday'='" + weekday + "'AND ((json->>'start_time')::json->>'hours')::integer * 60 + ((json->>'start_time')::json->>'minutes')::integer<=" + str(
                ts + 10) + "AND ((json->>'start_time')::json->>'hours')::integer * 60 + ((json->>'start_time')::json->>'minutes')::integer>" + str(
                ts - 90))
        if len(classes_by_time) != 0:
            time_slot = secrets.choice(classes_by_time)
            class_id = time_slot.id
            year = time_slot.json['year']
            group = secrets.choice(time_slot.json['groups'])
            studenta_by_group = students.query(
                "json->>'year'='" + str(year) + "'AND json->>'group'='" + str(group) + "'")
            if len(studenta_by_group) != 0:
                student = secrets.choice(studenta_by_group)
                student_id = student.id
                start_time = time_slot.json['start_time']
                time = int(start_time['hours']) * 60 + int(start_time['minutes'])
                if ts <= time + 5:
                    status = "in_time"
                else:
                    status = "late"
                attendance.put({"student_id": student_id, "class_id": class_id, "date": date, "status": status})
    print("Attendance created")


'''
select all late ppl
'''


def query1(db):
    table = db['attendance']
    file = open("file1", "w")
    output = table.query("json->>'status'='late'")
    file.write(str(len(output)))
    file.write(" late people in total.\n")
    for row in output:
        file.write(str(row))
        file.write("\n")
    file.close()


'''
select ppl who are not late
'''


def query2(db):
    table = db['attendance']
    file = open("file2", "w")
    output = table.query("json->>'status'='in_time'")
    file.write(str(len(output)))
    file.write(" people came on time.\n")
    for row in output:
        file.write(str(row))
        file.write("\n")
    file.close()


'''
select only ppl who are late by more than 10 minutes
'''


def query3(db):
    file = open("file3", "w")
    str1 = "select id as id, json->>'student_id' as student, json->>'class_id' as class, json->>'date' as date, json->>'status' as status from attendance where json->>'status' = 'late'"
    str2 = "select id as id, json->>'course' as course, json->>'type' as type, json->'start_time'->>'hours' as hours, json->'start_time'->>'minutes' as minutes from classes"
    str3 = "abs(extract(hour from cast(a.date as date)) * 60 + extract(minute from cast(a.date as date)) - cast(c.hours as int) * 60 - cast(c.minutes as int)) > 10"
    query = "select * from ({}) a inner join ({}) c on cast(a.class as int) = c.id and({})".format(str1, str2, str3)
    output = db.execute(query)
    file.write(str(len(output)))
    file.write(" people late by more than 10 minutes.\n")
    for row in output:
        file.write(str(row))
        file.write("\n")
    file.close()


'''
show popularity of each type of class for each course
'''


def query4(db):
    file = open("file4", "w")
    str1 = "select id as id, json->>'student_id' as student, json->>'class_id' as class from attendance"
    str2 = "select id as id, json->>'course' as course, json->>'type' as type from classes"
    query0 = "select * from ({}) a inner join ({}) c on cast(a.class as int) = c.id".format(str1, str2)
    query = "select course, type, count(*) as amount from ({}) as joined group by course, type order by course, amount desc".format(
        query0)
    output = db.execute(query)
    file.write("Popularity of classes:\n")
    for row in output:
        file.write(str(row))
        file.write("\n")
    file.close()


'''
average amount of people present
'''


def query5(db):
    file = open("file5", "w")
    str1 = "select id as id, json->>'student_id' as student, json->>'class_id' as class from attendance"
    str2 = "select id as id, json->>'course' as course, json->>'type' as type from classes"
    query0 = "select * from ({}) a inner join ({}) c on cast(a.class as int) = c.id".format(str1, str2)
    query = "select round(avg(amount), 0) from (select course, type, count(*) as amount from ({}) as joined group by course, type) as amount".format(
        query0)
    output = db.execute(query)
    file.write("Average amount of people among all classes:\n")
    for row in output:
        file.write(str(row))
        file.write("\n")
    file.close()


def main():
    client = PostgresNoSQLClient(host='localhost', port='8000', user='postgres', password='qweqweqwe')
    db = client['attendance_system']
    create_students(db)
    create_scanners(db)
    create_classes(db)
    create_attendance(db)
    query1(db)
    query2(db)
    query3(db)
    query4(db)
    query5(db)


if __name__ == '__main__':
    main()
