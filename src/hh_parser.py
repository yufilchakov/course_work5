import requests
from src import DBManager


def employers_parses(employer_ids: list[str], base_url) -> list:
    """Получение списка id работодателей"""
    headers = {'User-Agent': 'HH-User-Agent'}
    employers_info = []
    for employer_id in employer_ids:
        url = f'{base_url}/employers/{employer_id}'
        print(f"Making request to URL: {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            employer_info = response.json()
            if employer_info:
                employers_info.append(employer_info)
        else:
            print(f"Ошибка при запросе к API: {response.status_code}")
    return employers_info

    
def vacancies_parser(employer_ids: list[str], hh_url) -> list:
    """Функция для проверки наличия вакансий у всех работодателей."""
    headers = {'User-Agent': 'HH-User-Agent'}
    all_vacancies = []
    for employer_id in employer_ids:
        params = {'employer_id': employer_id}
        response = requests.get(f"{hh_url}/vacancies", headers=headers, params=params)
        if response.status_code == 200:
            vacancies_info = response.json()
            vacancies_list = vacancies_info.get('items', [])
            if vacancies_list:
                all_vacancies.extend(vacancies_list)
        else:
            print(f"Ошибка при запросе к API: {response.status_code}")
    return all_vacancies


def save_employers_to_db(employers_info: list, dbmanager: DBManager):
    """Сохранение информации о работодателях в базу данных."""
    for employer_info in employers_info:
        employer_data = {
            'employer_id': employer_info.get('id'),
            'name': employer_info.get('name'),
            'url': employer_info.get('site', {}).get('alternate_url'),
            'open_vacancies': employer_info.get('open_vacancies')
        }
        if not dbmanager.check_employer_exists(employer_data['employer_id']):
            dbmanager.insert_employer_info(employer_data)
            print(f"Работодатель с ID {employer_data['employer_id']} успешно добавлен.")
        else:
            dbmanager.update_employer_info(employer_data)
            print(f"Информация о работодателе с ID {employer_data['employer_id']} обновлена.")


def save_vacancies_info_to_db(vacancies_list: list, dbmanager: DBManager):
    """Сохранение информации о вакансиях в базу данных."""
    for vacancy in vacancies_list:
        employer_id = vacancy.get('employer', {}).get('id')
        if dbmanager.check_employer_exists(employer_id):
            vacancy_data = {
                'title': vacancy.get('name'),
                'company': vacancy.get('employer', {}).get('name'),
                'salary': vacancy.get('from'),
                'description': vacancy.get('description'),
                'employer_id': employer_id,
            }
            dbmanager.insert_vacancy_info(vacancy_data)
        else:
            print(f"Работодатель с ID {employer_id} не найден в таблице 'employer'.")
            