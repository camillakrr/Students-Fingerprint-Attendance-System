import random
import secrets
from datetime import datetime

import names
from pg4nosql.PostgresNoSQLClient import PostgresNoSQLClient


def create_students(db):
    students = db['students']
    fingerprints = []
    for i in range(0, 1000):
        first_name = names.get_first_name()
        last_name = names.get_last_name()
        year = random.randint(1, 4)
        group = random.randint(1, 10)
        fingerprint = secrets.token_hex(nbytes=16)
        fingerprints.append(fingerprint)
        students.put({'first_name': first_name, 'last_name': last_name, 'year': year, 'group': group,
                      'fingerprint': fingerprint},
                     auto_commit=False)
    students.commit()
    return fingerprints


def create_readers(db):
    readers = db['readers']
    serial_numbers = []
    for i in range(100, 200):
        serial_number = random.randint(10000000000, 99999999999)
        serial_numbers.append(serial_number)
        readers.put({'room': i, 'serial_number': serial_number}, auto_commit=False)
    readers.commit()
    return serial_numbers


def create_timetable(db):
    timetable = db['timetable']
    courses = ['Discrete Math and Logic', 'Data Structure and Algorithms', 'Analitical Geometry and Linear Algebra',
               'English', 'Introduction to Programming', 'Mathematical Analysis', 'Introduction to AI',
               'Data Modeling and Databases', 'Software Project', 'Probability and Statistics', 'Networks',
               'Control Theory', 'Digital Signal Processing', 'Information Retrieval', 'Data Mining',
               'Lean Software Development', 'Mechanics and Machines', 'Software System Design', 'Life Safety',
               'Practical Machine Learning and Deep Learning', 'Software Quality and Reliability']
    types = ['lecture', 'tutorial', 'lab']
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    timeslots = ['9:00', '10:35', '12:10', '14:10', '15:45', '17:20', '18:55', '20:30']
    rooms = dict()
    groups = dict()
    for i in range(1000):
        course = secrets.choice(courses)
        type = secrets.choice(types)
        room = random.randint(100, 200)
        year = random.randint(1, 4)
        group = random.randint(1, 10)
        weekday = secrets.choice(weekdays)
        time = secrets.choice(timeslots)
        timeSplit = time.split(":")
        ts = int(timeSplit[0]) * 60 + int(timeSplit[1])
        if type == "lab":
            if weekday + time + str(room) not in rooms and weekday + time + str(year) + str(group) not in groups:
                rooms[weekday + time + str(room)] = "true"
                groups[weekday + time + str(year) + str(group)] = "true"
                timetable.put(
                    {"course": course, "type": type, "room": room, "year": year, "groups": "{" + str(group) + "}",
                     "weekday": weekday, "time": time, "ts": ts})
        else:
            if weekday + time + str(room) not in rooms:
                for i in range(10):
                    if weekday + time + str(year) + str(i + 1) in groups:
                        break
                if i == 9:
                    rooms[weekday + time + str(room)] = "true"
                    for j in range(10):
                        groups[weekday + time + str(year) + str(j + 1)] = "true"
                    timetable.put(
                        {"course": course, "type": type, "room": room, "year": year, "groups": "{1,2,3,4,5,6,7,8,9,10}",
                         "weekday": weekday, "time": time, "ts": ts})


def create_attendance(db, fingerprints, serial_numbers):
    students = db['students']
    readers = db['readers']
    timetable = db['timetable']
    attendance = db['attendance']
    for i in range(100000):
        fingerprint = secrets.choice(fingerprints)
        student = students.query("json->>'fingerprint'='" + fingerprint + "'")[0]
        student_id = student.id
        year = student.json['year']
        group = student.json['group']
        serial_number = secrets.choice(serial_numbers)
        reader = readers.query("json->>'serial_number'='" + str(serial_number) + "'")[0]
        room = reader.json['room']
        unixtime = random.randint(1493078400, 1587772800)
        time = datetime.utcfromtimestamp(unixtime).strftime('%d.%m.%Y %H:%M')
        weekday = datetime.fromtimestamp(unixtime).strftime("%A")
        hours = int(datetime.fromtimestamp(unixtime).strftime("%H")) - 3
        minutes = int(datetime.fromtimestamp(unixtime).strftime("%M"))
        ts = hours * 60 + minutes
        timeslot = timetable.query("json->>'room'='" + str(room) + "'AND json->>'year'='" + str(
            year) + "'AND array_position((json->>'groups')::integer[]," + str(
            group) + ") IS NOT NULL AND json->>'weekday'='" + weekday + "'AND (json->>'ts')::integer<" + str(
            ts + 5) + "AND (json->>'ts')::integer>" + str(ts - 90))
        if len(timeslot) == 1:
            class_id = timeslot[0].id
            start_time = timeslot[0].json['ts']
            if ts <= start_time:
                lateness = 0
            else:
                lateness = ts - start_time
            attendance.put(
                {"student_id": student_id, "class_id": class_id, "time": time, "lateness": lateness})


def main():
    client = PostgresNoSQLClient(host='localhost', port='8000', user='postgres', password='qweqweqwe')
    db = client['attendance_system']
    fingerprints = create_students(db)
    serial_numbers = create_readers(db)
    create_timetable(db)
    create_attendance(db, fingerprints, serial_numbers)


if __name__ == '__main__':
    main()
