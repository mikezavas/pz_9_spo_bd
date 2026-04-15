from main import SQLTable

db_config = {
    'host': 'srv221-h-st.jino.ru',
    'user': 'j30084097_13418',
    'password': 'pPS090207/()',
    'database': 'j30084097_13418',
    'port': 3306
}

if __name__ == "__main__":
    TBL_STUDENTS = 'test_students'
    TBL_COURSES = 'test_courses'

    db = SQLTable(db_config, TBL_STUDENTS, engine='mysql')

    db.drop_table()
    db.create_table('id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), grade INT')

    db.insert({'name': 'Napoleon', 'grade': 85})
    db.insert({'name': 'Putin', 'grade': 90})
    db.insert({'name': 'Trump', 'grade': 78})

    print("\nСтуденты с grade > 80:")
    for s in db.select(filters={'grade': ('>', 80)}):
        print(s)

    db_courses = SQLTable(db_config, TBL_COURSES, engine='mysql')
    db_courses.drop_table()
    db_courses.create_table('id INT AUTO_INCREMENT PRIMARY KEY, student_id INT, subject VARCHAR(50)')

    db_courses.insert({'student_id': 1, 'subject': 'History'})
    db_courses.insert({'student_id': 2, 'subject': 'Economics'})

    print("\nINNER JOIN:")
    # Передаем полные имена таблиц в условиях и колонках
    join_res = db.join_query(
        TBL_COURSES,
        f'{TBL_STUDENTS}.id = {TBL_COURSES}.student_id',
        columns=f'{TBL_STUDENTS}.name, {TBL_COURSES}.subject',
        join_type='INNER'
    )
    for r in join_res:
        print(r)

    print("\nUNION (имена + предметы):")
    q1 = (f"SELECT name as value FROM {TBL_STUDENTS}", [])
    q2 = (f"SELECT subject as value FROM {TBL_COURSES}", [])
    union_res = db.union_query([q1, q2], distinct=True)
    for r in union_res:
        print(r)

    db_courses.drop_table()
    db.drop_table()
    db.disconnect()
