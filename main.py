from src.DBManager import DBManager
from src.config import config_parser
from src.constants import EMPLOYER_IDS, HH_URL
from src.hh_parser import employers_parses, vacancies_parser, save_vacancies_info_to_db


config = config_parser('database.ini')
dbmanager = DBManager(config)
dbmanager.create_db()
dbmanager.create_table()
employers_info = employers_parses(EMPLOYER_IDS, HH_URL)
for employer in employers_info:
    dbmanager.insert_employers_info(employer)
vacancies_list = vacancies_parser(EMPLOYER_IDS, HH_URL)
save_vacancies_info_to_db(vacancies_list, dbmanager)


def main():
    while True:
        print(f'Выберите запрос или выберите "Выход":\n'
              f'1 Список всех компаний и количество вакансий у каждой компании\n'
              f'2 Cписок всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на '
              f'вакансию\n'
              f'3 Средняя зарплата по вакансиям\n'
              f'4 Список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
              f'5 Список всех вакансий, в названии которых содержатся запрашиваемое слово\n')
        choice = input()
        if choice == '1':
            companies_vacancies_count = dbmanager.get_companies_and_vacancies_count()
            print(f'Список всех компаний и количество вакансий у каждой компании: {companies_vacancies_count}')
        elif choice == '2':
            vacancy_list = dbmanager.get_all_vacancies()
            print(f'Cписок всех вакансий с указанием названия компании, вакансии, зарплаты и ссылки на вакансию: '
                  f'{vacancy_list}')
        elif choice == '3':
            avg_salary = dbmanager.get_avg_salary()
            print(f'Средняя зарплату по вакансиям: {avg_salary}')
        elif choice == '4':
            vacancies_with_higher_salary = dbmanager.get_vacancies_with_higher_salary(0)
            print(f'Список всех вакансий, у которых зарплата выше средней по всем вакансиям: '
                  f'{vacancies_with_higher_salary}')
        elif choice == '5':
            user_input = input(f'Введите слово: ')
            vacancies_with_keyword = dbmanager.get_vacancies_with_keyword(user_input)
            print(f'Cписок всех вакансий, в названии которых содержатся {user_input}: {vacancies_with_keyword}')
        elif choice == "Выход":
            break
        else:
            print(f'Введён неверный запрос')
        
        
if __name__ == '__main__':
    main()
