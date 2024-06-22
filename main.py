from src.DBManager import DBManager
from src.config import config_parser
from src.constants import EMPLOYER_IDS, HH_URL
from src.hh_parser import employers_parses, vacancies_parser, save_vacancies_info_to_db


def main():
    config = config_parser('database.ini')
    dbmanager = DBManager(config)
    dbmanager.create_db()
    dbmanager.create_table()
    employers_info = employers_parses(EMPLOYER_IDS, HH_URL)
    for employer in employers_info:
        dbmanager.insert_employers_info(employer)
    vacancies_list = vacancies_parser(EMPLOYER_IDS, HH_URL)
    save_vacancies_info_to_db(vacancies_list, dbmanager)
    dbmanager.get_companies_and_vacancies_count()
    dbmanager.get_all_vacancies()
    dbmanager.get_avg_salary()
    dbmanager.get_vacancies_with_higher_salary(50000)
    dbmanager.get_vacancies_with_keyword('python')
    
    
if __name__ == '__main__':
    main()
