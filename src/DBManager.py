import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DBManager:
    
    def __init__(self, config, dbname='course_work5'):
        self.config = config
        self.dbname = dbname
        self.conn = None
        self.cursor = None
    
    def create_db(self):
        """Создаем базу данных"""
        connection = psycopg2.connect(**self.config)
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f'DROP DATABASE IF EXISTS {self.dbname}')
        cursor.execute(f'CREATE DATABASE {self.dbname};')
        
    def create_table(self):
        """Создаем таблицы в базе данных."""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                create_employers_table_query = """
                    CREATE TABLE IF NOT EXISTS employer (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    url VARCHAR(255),
                    open_vacancies INTEGER
                    );
                    """
                cursor.execute(create_employers_table_query)
                create_vacancies_table_query = """
                    CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(100),
                    company VARCHAR(100),
                    salary INT,
                    description TEXT,
                    employer_id INTEGER REFERENCES employer(id)
                    );
                    """
                cursor.execute(create_vacancies_table_query)
                conn.commit()
    
    def insert_values(self, values, key):
        """Функция отвечает за вставку данных в базу данных"""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                try:
                    if key == 'employer':
                        insert_query = """
                        INSERT INTO employer (id, name, url, open_vacancies) VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(insert_query, values)
                        logger.info("Employer values inserted successfully.")
                    elif key == 'vacancies':
                        insert_query = """
                        INSERT INTO vacancies (title, company, salary_from, description, employer_id)
                        VALUES (%s, %s, %s, %s, %s)"""
                        cursor.execute(insert_query, values)
                        logger.info("Vacancies values inserted successfully.")
                    conn.commit()
                    return True
                except psycopg2.IntegrityError as integrity_err:
                    logger.error(f'Ошибка целостности данных: {integrity_err}')
                    conn.rollback()
                except psycopg2.DatabaseError as db_err:
                    logger.error(f'Ошибка базы данных: {db_err}')
                    conn.rollback()
                except Exception as e:
                    logger.error(f'Неизвестная ошибка: {e}')
                    conn.rollback()
                return False
    
    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT e.name, COUNT(v.id) as vacancies_count
                FROM employer e
                LEFT JOIN vacancies v ON e.id = v.employer_id
                GROUP BY e.name
                ORDER BY vacancies_count DESC;
                """
                cursor.execute(query)
                results = cursor.fetchall()
        return results
    
    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
                вакансию."""
        self.config['dbname'] = self.dbname
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()
        query = """
        SELECT id, title, employer_id FROM vacancies;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.cursor.close()
        self.conn.close()
        return results
    
    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT AVG(salary) FROM vacancies
                """
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else None
    
    def get_vacancies_with_higher_salary(self, high_salary):
        """Получает список всех вакансий, у которых зарплата выше заданного порога."""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT title, salary FROM vacancies
                WHERE salary > %s
                """
                cursor.execute(query, (high_salary,))
                results = cursor.fetchall()
                return results
    
    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        self.config['dbname'] = self.dbname
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()
        query = """
                  SELECT id, title, salary FROM vacancies WHERE title ILIKE %s;
                  """
        search_pattern = f"%{keyword}%"
        self.cursor.execute(query, (search_pattern,))
        results = self.cursor.fetchall()
        self.cursor.close()
        self.conn.close()
        return results
    
    def insert_employers_info(self, employer):
        """Метод для вставки информации о работодателе в базу данных."""
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                values = (
                    employer.get('id'),
                    employer.get('name'),
                    employer.get('alternate_url'),
                    employer.get('open_vacancies')
                )
                query = """
                INSERT INTO employer (id, name, url, open_vacancies) VALUES (%s, %s, %s, %s)
                """
                cursor.execute(query, values)
                conn.commit()
    
    def insert_vacancy_info(self, vacancy_data):
        """Метод для вставки информации о вакансии в базу данных."""
        required_keys = ['title', 'description', 'salary', 'employer_id', 'company']
        missing_keys = [key for key in required_keys if key not in vacancy_data or vacancy_data[key] is None]
        if missing_keys:
            if 'description' in missing_keys:
                vacancy_data['description'] = 'Описание отсутствует'
            else:
                raise ValueError(f"Отсутствуют необходимые ключи или их значения: {missing_keys}")
        
        salary_from = vacancy_data.get('salary_from')
        salary_to = vacancy_data.get('salary_to')
        if salary_from and salary_to and str(salary_from).isdigit() and str(salary_to).isdigit():
            salary = (int(salary_from) + int(salary_to))
        else:
            salary = None
        
        self.config['dbname'] = self.dbname
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                query = """
                       INSERT INTO vacancies (title, description, company, salary, employer_id)
                       VALUES (%s, %s, %s, %s, %s)
                       """
                values = (vacancy_data['title'], vacancy_data['description'], vacancy_data['company'],
                          salary, vacancy_data['employer_id'])
                try:
                    cursor.execute(query, values)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"Произошла ошибка при вставке данных: {e}")
                    raise
    
    def check_employer_exists(self, employer_id) -> bool:
        """ Метод проверяет наличие работодателя в базе данных по его идентификатору."""
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                query = "SELECT EXISTS(SELECT 1 FROM employer WHERE id = %s)"
                cursor.execute(query, (employer_id,))
                return cursor.fetchone()[0]
      
    def close_connection(self):
        """Функция предназначена для закрытия соединения с базой данных и курсора."""
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            