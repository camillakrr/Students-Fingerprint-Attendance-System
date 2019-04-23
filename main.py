import secrets

import names
from pg4nosql.PostgresNoSQLClient import PostgresNoSQLClient

groups = ["B18-01", "B18-02", "B18-03", "B18-04", "B18-05", "B18-06",
          "B17-01", "B17-02", "B17-03", "B17-04", "B17-05", "B17-06", "B17-07", "B17-08",
          "B16-DS-01", "B16-DS-02", "B16-RO-01", "B16-SE-01",
          "B15-01", "B15-02" "B15-03",
          "M18-DS-01", "B18-DS-02", "M18-RO-01", "M18-RO-02", "M18-SE-01", "M18-SE-02"]


def generate_students(db):
    students = db['students']
    for i in range(0, 1000):
        first_name = names.get_first_name()
        last_name = names.get_last_name()
        group = secrets.choice(groups)
        fingerprint = secrets.token_hex(nbytes=16)
        students.put({'first_name': first_name, 'last_name': last_name, 'group': group, 'fingerprint': fingerprint},
                     auto_commit=False)
    students.commit()


def main():
    client = PostgresNoSQLClient(host='localhost', port='8000', user='postgres', password='qweqweqwe')
    db = client['attendance_system']
    generate_students(db)


if __name__ == '__main__':
    main()
