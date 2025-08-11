import sys
import sqlite3
from datetime import datetime, date
import random
import time
from typing import List

class Employee:
    def __init__(self, full_name: str, birth_date: str, gender: str):
        self.full_name = full_name
        self.birth_date = birth_date
        self.gender = gender
        
    def save_to_db(self, cursor):
        cursor.execute(
            "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)",
            (self.full_name, self.birth_date, self.gender)
        )
    
    def calculate_age(self) -> int:
        birth_date = datetime.strptime(self.birth_date, "%Y-%m-%d").date()
        today = date.today()
        age = today.year - birth_date.year
        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1
        return age
    
    @staticmethod
    def batch_save(cursor, employees: List['Employee']):
        data = [(e.full_name, e.birth_date, e.gender) for e in employees]
        cursor.executemany(
            "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)",
            data
        )

class EmployeeDirectory:
    def __init__(self, db_name: str = "employees.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        
    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                birth_date TEXT NOT NULL,
                gender TEXT NOT NULL
            )
        """)
        self.conn.commit()
        print("\n[SUCCESS] Employee table created successfully\n")
    
    def clear_database(self):
        self.cursor.execute("DROP TABLE IF EXISTS employees")
        self.conn.commit()
        self.create_table()
        print("\n[WARNING] Database has been cleared!\n")
    
    def add_employee(self, full_name: str, birth_date: str, gender: str):
        employee = Employee(full_name, birth_date, gender)
        employee.save_to_db(self.cursor)
        self.conn.commit()
        print(f"\n[SUCCESS] Employee '{full_name}' added successfully\n")
    
    def display_all_employees(self):
        self.cursor.execute("""
            SELECT DISTINCT full_name, birth_date, gender 
            FROM employees 
            GROUP BY full_name, birth_date
            ORDER BY full_name
        """)
        employees = self.cursor.fetchall()
        
        print("\n" + "="*80)
        print("EMPLOYEE DIRECTORY".center(80))
        print("="*80)
        print(f"{'FULL NAME':<40} | {'BIRTH DATE':<12} | {'GENDER':<6} | {'AGE'}")
        print("-"*80)
        
        for emp in employees:
            age = Employee(emp[0], emp[1], emp[2]).calculate_age()
            print(f"{emp[0]:<40} | {emp[1]:<12} | {emp[2]:<6} | {age}")
        
        print("="*80 + "\n")
    
    def generate_random_employees(self, count: int = 1000000):
        print(f"\n[INFO] Generating {count:,} random employees...\n")
        
        first_names_male = ["Ivan", "Petr", "Sergey", "Andrey", "Alexey", "Dmitry", "Mikhail", "Nikolay"]
        first_names_female = ["Anna", "Maria", "Elena", "Olga", "Tatyana", "Natalia", "Irina", "Svetlana"]
        last_names = ["Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Fedorov"]
        middle_names = ["Ivanovich", "Petrovich", "Sergeevich", "Andreevich", "Alexeevich", 
                       "Ivanovna", "Petrovna", "Sergeevna", "Andreevna", "Alexeevna"]
        
        employees = []
        
        for i in range(count):
            gender = random.choice(["Male", "Female"])
            if gender == "Male":
                first_name = random.choice(first_names_male)
                middle_name = random.choice(middle_names[:5])
            else:
                first_name = random.choice(first_names_female)
                middle_name = random.choice(middle_names[5:])
            
            last_name = random.choice(last_names)
            full_name = f"{last_name} {first_name} {middle_name}"
            
            year = random.randint(1950, 2005)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            birth_date = f"{year}-{month:02d}-{day:02d}"
            
            employees.append(Employee(full_name, birth_date, gender))
            
            if len(employees) >= 1000:
                Employee.batch_save(self.cursor, employees)
                self.conn.commit()
                employees = []
        
        if employees:
            Employee.batch_save(self.cursor, employees)
            self.conn.commit()
        
        print("\n[INFO] Generating 100 male employees with last names starting with 'F'...\n")
        f_employees = []
        for i in range(100):
            first_name = random.choice(first_names_male)
            middle_name = random.choice(middle_names[:5])
            last_name = "F" + random.choice(["isher", "ord", "letcher", "ranklin", "erguson"])
            full_name = f"{last_name} {first_name} {middle_name}"
            
            year = random.randint(1950, 2005)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            birth_date = f"{year}-{month:02d}-{day:02d}"
            
            f_employees.append(Employee(full_name, birth_date, "Male"))
        
        Employee.batch_save(self.cursor, f_employees)
        self.conn.commit()
        
        print("\n[SUCCESS] Data generation completed successfully\n")
    
    def query_male_f_surnames(self) -> float:
        start_time = time.time()
        
        self.cursor.execute("""
            SELECT full_name, birth_date, gender 
            FROM employees 
            WHERE gender = 'Male' AND full_name LIKE 'F%'
        """)
        results = self.cursor.fetchall()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\n" + "="*80)
        print("MALE EMPLOYEES WITH 'F' SURNAMES".center(80))
        print("="*80)
        print(f"{'FULL NAME':<40} | {'BIRTH DATE':<12} | {'GENDER':<6} | {'AGE'}")
        print("-"*80)
        
        for emp in results[:10]:
            age = Employee(emp[0], emp[1], emp[2]).calculate_age()
            print(f"{emp[0]:<40} | {emp[1]:<12} | {emp[2]:<6} | {age}")
        
        if len(results) > 10:
            print(f"... and {len(results) - 10} more records")
        
        print(f"\n[INFO] Query executed in {execution_time:.4f} seconds")
        print("="*80 + "\n")
        return execution_time
    
    def optimize_database(self):
        print("\n[INFO] Optimizing database...")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_gender_name ON employees(gender, full_name)")
        self.conn.commit()
        print("[SUCCESS] Database optimization completed (created index on gender and full_name)\n")
    
    def close(self):
        self.conn.close()

def main():
    if len(sys.argv) < 2:
        print("\nEMPLOYEE DIRECTORY SYSTEM")
        print("="*50)
        print("Usage: python app.py <mode> [arguments]")
        print("\nMODES:")
        print("1 - Create table")
        print("2 'Full Name' 'YYYY-MM-DD' 'Gender' - Add employee")
        print("3 - Display all employees")
        print("4 - Generate random employees")
        print("5 - Query male employees with F surnames")
        print("6 - Optimize database and test query")
        print("7 - CLEAR DATABASE (DANGER!)")
        print("="*50 + "\n")
        return
    
    mode = sys.argv[1]
    directory = EmployeeDirectory()
    
    try:
        if mode == "1":
            directory.create_table()
        elif mode == "2":
            if len(sys.argv) < 5:
                print("\n[ERROR] Missing arguments")
                print("Usage: python app.py 2 'Full Name' 'YYYY-MM-DD' 'Gender'\n")
                return
            full_name = sys.argv[2]
            birth_date = sys.argv[3]
            gender = sys.argv[4]
            directory.add_employee(full_name, birth_date, gender)
        elif mode == "3":
            directory.display_all_employees()
        elif mode == "4":
            directory.generate_random_employees()
        elif mode == "5":
            directory.query_male_f_surnames()
        elif mode == "6":
            print("\n[TEST] Running query before optimization...")
            time_before = directory.query_male_f_surnames()
            
            directory.optimize_database()
            
            print("[TEST] Running query after optimization...")
            time_after = directory.query_male_f_surnames()
            
            print("\n" + "="*50)
            print("OPTIMIZATION RESULTS".center(50))
            print("="*50)
            print(f"Before: {time_before:.4f} seconds")
            print(f"After:  {time_after:.4f} seconds")
            improvement = (time_before - time_after)/time_before * 100
            print(f"Improvement: {improvement:.2f}% faster")
            print("="*50 + "\n")
        elif mode == "7":
            confirm = input("\n[WARNING] Are you sure you want to clear the database? (y/n): ")
            if confirm.lower() == 'y':
                directory.clear_database()
            else:
                print("\n[INFO] Operation cancelled\n")
        else:
            print("\n[ERROR] Invalid mode. Please use 1-7\n")
    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}\n")
    finally:
        directory.close()

if __name__ == "__main__":
    main()