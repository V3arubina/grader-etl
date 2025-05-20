import requests
import logging
import os
import ast
import json
from datetime import datetime, timedelta, timezone
import psycopg2
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

import smtplib
import ssl
from email.message import EmailMessage

# Загрузка переменных окружения из файла .env
load_dotenv()

# Константы
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
API_URL = "https://b2b.itresume.ru/api/statistics"
CLIENT = "Skillfactory"
CLIENT_KEY = "M2MGWS"
DAYS_BACK = 7

# Параметры подключения к БД теперь читаются из .env для безопасности
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "grader"),
    "user": os.getenv("DB_USER", "apple"),
    "password": os.getenv("DB_PASSWORD", ""),  
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

# Путь к файлу с учетными данными Google Sheets
GSHEETS_CREDENTIALS_PATH = "credentials/your_credentials.json"
GSHEETS_SPREADSHEET_NAME = "Grader Raw Data"
GSHEETS_RAW_DATA_WORKSHEET_NAME = "Raw Data"
GSHEETS_DAILY_REPORT_WORKSHEET_NAME = "Daily Report"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1DTuFV1NAbd6vevFOJM6POzekFZaBVDtR9ULRCI1Igro/edit?gid=0#gid=0"

EMAIL_SENDER = os.getenv("EMAIL_SENDER") 
EMAIL_SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD") 
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER") 
EMAIL_SMTP_SERVER = "smtp.gmail.com" 
EMAIL_SMTP_PORT = 465 

REPORT_METRICS_HEADERS = [
    ["Метрика", "Значение"],
    ["Дата отчета", None],
    ["Всего попыток", None],
    ["Успешных попыток", None],
    ["Процент успешных попыток", None],
    ["Уникальных пользователей", None],
    ["Попыток типа 'run'", None],
    ["Попыток типа 'check'", None]
]

def setup_logging():
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_filename = os.path.join(LOG_DIR, f"app_{today_str}.log")
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
        filemode='a'
    )

def delete_old_logs():
    now = datetime.now()
    cutoff = now - timedelta(days=3)
    for filename in os.listdir(LOG_DIR):
        if filename.startswith("app_") and filename.endswith(".log"):
            try:
                log_date_str = filename[4:14]
                log_date = datetime.strptime(log_date_str, "%Y-%m-%d")
                if log_date < cutoff:
                    os.remove(os.path.join(LOG_DIR, filename))
                    logging.info(f"Удален старый лог-файл: {filename}")
            except ValueError:
                pass

def get_gspread_client(credentials_path):
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
    return gspread.authorize(creds)

def safe_parse_passback(passback_str):
    try:
        return json.loads(passback_str)
    except Exception:
        try:
            return ast.literal_eval(passback_str)
        except Exception:
            return {}

def fetch_data():
    today = datetime.now(timezone.utc)
    start_date = today - timedelta(days=DAYS_BACK)
    params = {
        "client": CLIENT,
        "client_key": CLIENT_KEY,
        "start": start_date.isoformat(),
        "end": today.isoformat(),
    }
    logging.info("Начат запрос данных к API.")
    try:
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Данные успешно получены, записей: {len(data)}")
        return data
    except requests.RequestException as e:
        logging.error(f"Ошибка при обращении к API ({type(e).__name__}): {e}")
        print(f"Ошибка при обращении к API: {e}")
    except ValueError as e:
        logging.error(f"Ошибка при разборе JSON ({type(e).__name__}): {e}")
        print(f"Ошибка при разборе данных: {e}")
    return None

def process_data(data):
    processed_records = []
    if not data:
        logging.warning("Нет данных для обработки.")
        return processed_records
    for record in data:
        try:
            lti_user_id = record.get("lti_user_id")
            passback_str = record.get("passback_params", "{}")
            passback = safe_parse_passback(passback_str) if passback_str else {}
            oauth_consumer_key = passback.get("oauth_consumer_key")
            lis_result_sourcedid = passback.get("lis_result_sourcedid")
            lis_outcome_service_url = passback.get("lis_outcome_service_url")
            attempt_type = record.get("attempt_type")
            created_at = record.get("created_at")
            raw_is_correct = record.get("is_correct")
            is_correct = None
            if raw_is_correct is not None:
                is_correct = bool(raw_is_correct)
            if not isinstance(lti_user_id, str) or not lti_user_id:
                logging.warning(f"Пропущена запись: некорректный user_id: {lti_user_id}")
                continue
            if oauth_consumer_key is not None and not isinstance(oauth_consumer_key, str):
                logging.warning(f"Пропущена запись: некорректный oauth_consumer_key: {oauth_consumer_key}")
                continue
            if lis_result_sourcedid is not None and not isinstance(lis_result_sourcedid, str):
                logging.warning(f"Пропущена запись: некорректный lis_result_sourcedid: {lis_result_sourcedid}")
                continue
            if lis_outcome_service_url is not None and not isinstance(lis_outcome_service_url, str):
                logging.warning(f"Пропущена запись: некорректный lis_outcome_service_url: {lis_outcome_service_url}")
                continue
            if attempt_type is not None and not isinstance(attempt_type, str):
                logging.warning(f"Пропущена запись: некорректный attempt_type: {attempt_type}")
                continue
            if not isinstance(created_at, str):
                logging.warning(f"Пропущена запись: некорректный created_at: {created_at}")
                continue
            processed_records.append({
                "user_id": lti_user_id,
                "oauth_consumer_key": oauth_consumer_key,
                "lis_result_sourcedid": lis_result_sourcedid,
                "lis_outcome_service_url": lis_outcome_service_url,
                "is_correct": is_correct,
                "attempt_type": attempt_type,
                "event_timestamp": created_at,
            })
        except (ValueError, SyntaxError) as e:
            logging.warning(f"Ошибка при обработке записи ({type(e).__name__}): {e}, запись: {record}")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при обработке записи ({type(e).__name__}): {e}, запись: {record}")
    logging.info(f"Обработано {len(data)} записей, успешно подготовлено {len(processed_records)}.")
    return processed_records

def save_to_db(data):
    if not data:
        logging.warning("Нет данных для записи в БД.")
        return
    conn = None
    cur = None
    count = 0
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        for record in data:
            try:
                cur.execute("""
                    INSERT INTO statistics (
                        user_id,
                        oauth_consumer_key,
                        lis_result_sourcedid,
                        lis_outcome_service_url,
                        is_correct,
                        attempt_type,
                        event_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, event_timestamp) DO NOTHING
                """, (
                    record.get("user_id"),
                    record.get("oauth_consumer_key"),
                    record.get("lis_result_sourcedid"),
                    record.get("lis_outcome_service_url"),
                    record.get("is_correct"),
                    record.get("attempt_type"),
                    record.get("event_timestamp"),
                ))
                count += 1
            except psycopg2.Error as e:
                logging.warning(f"Ошибка при вставке записи ({type(e).__name__}): {e}, данные: {record}")
                conn.rollback()
                continue
        conn.commit()
        logging.info(f"Успешно записано {count} записей в базу.")
        print(f"Данные успешно загружены в базу данных: {count}")
    except psycopg2.Error as e:
        logging.error(f"Ошибка при подключении к БД ({type(e).__name__}): {e}")
        print(f"Ошибка при загрузке в БД: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Соединение с БД закрыто.")

def upload_raw_data_to_sheets(data, spreadsheet_name=GSHEETS_SPREADSHEET_NAME, worksheet_name=GSHEETS_RAW_DATA_WORKSHEET_NAME, credentials_path=GSHEETS_CREDENTIALS_PATH):
    print(f"Попытка загрузить данные в Google Sheets на лист '{worksheet_name}'...")
    try:
        gc = get_gspread_client(credentials_path)
        print(f"Авторизация в Google Sheets прошла успешно. Ищу таблицу: '{spreadsheet_name}'")
        try:
            spreadsheet = gc.open(spreadsheet_name)
            print(f"Таблица '{spreadsheet_name}' открыта успешно. URL таблицы: {spreadsheet.url}")
        except gspread.SpreadsheetNotFound:
            spreadsheet = gc.create(spreadsheet_name)
            print(f"Создана новая таблица: '{spreadsheet_name}'. URL таблицы: {spreadsheet.url}")

        spreadsheet.share('zadorozhnaya2596@gmail.com', perm_type='user', role='writer')

        print(f"Ищу лист: '{worksheet_name}' в таблице: '{spreadsheet_name}'")
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"Лист '{worksheet_name}' найден.")
        except gspread.WorksheetNotFound:
            num_rows = max(len(data) + 1, 1000)
            num_cols = max(len(data[0]) if data else 1, 20)
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=num_rows, cols=num_cols)
            print(f"Создан новый лист: '{worksheet_name}' в таблице: '{spreadsheet_name}'.")

        if data:
            header = list(data[0].keys())
            values = [header] + [list(record.values()) for record in data]
            print(f"Подготовлено {len(values)} строк для загрузки.")
        else:
            values = [["Нет данных"]]
            print("Нет данных для загрузки в Google Sheets.")

        worksheet.clear()
        print("Лист очищен. Загружаю данные...")
        worksheet.update(values, "A1")
        print(f"Сырые данные успешно загружены в Google Sheets: '{spreadsheet_name}' -> '{worksheet_name}'")
    except Exception as e:
        logging.error(f"Ошибка при загрузке сырых данных в Google Sheets: {e}")
        print(f"Ошибка при загрузке сырых данных в Google Sheets: {e}")

def generate_daily_report_and_send_to_sheets(spreadsheet_name=GSHEETS_SPREADSHEET_NAME, worksheet_name=GSHEETS_DAILY_REPORT_WORKSHEET_NAME, credentials_path=GSHEETS_CREDENTIALS_PATH):
    logging.info("Начато создание ежедневного отчета.")
    print("Начато создание ежедневного отчета...")
    conn = None
    report_values = {}
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        query = """
        SELECT
            CURRENT_DATE AS report_date,
            COUNT(*) AS total_attempts,
            COUNT(CASE WHEN is_correct = TRUE THEN 1 END) AS successful_attempts,
            ROUND(CAST(COUNT(CASE WHEN is_correct = TRUE THEN 1 END) AS NUMERIC) * 100 / COUNT(*), 2) AS success_percentage,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(CASE WHEN attempt_type = 'run' THEN 1 END) AS run_attempts,
            COUNT(CASE WHEN attempt_type = 'check' THEN 1 END) AS check_attempts
        FROM statistics
        WHERE CAST(event_timestamp AS DATE) = CURRENT_DATE;
        """
        cur.execute(query)
        report_row = cur.fetchone()

        if not report_row or report_row[1] == 0:
            logging.info("Нет данных за сегодняшний день для отчета.")
            print("Нет данных за сегодняшний день для отчета.")
            report_date_str = datetime.now().strftime("%Y-%m-%d")
            report_data = [
                ["Метрика", "Значение"],
                ["Дата отчета", report_date_str],
                ["Всего попыток", 0],
                ["Успешных попыток", 0],
                ["Процент успешных попыток", "0.00%"],
                ["Уникальных пользователей", 0],
                ["Попыток типа 'run'", 0],
                ["Попыток типа 'check'", 0]
            ]
            report_values = {
                "report_date": report_date_str,
                "total_attempts": 0,
                "successful_attempts": 0,
                "success_percentage": "0.00%",
                "unique_users": 0,
                "run_attempts": 0,
                "check_attempts": 0
            }
        else:
            report_date_str = report_row[0].strftime("%Y-%m-%d")
            total_attempts = report_row[1]
            successful_attempts = report_row[2]
            success_percentage = f"{report_row[3]:.2f}%"
            unique_users = report_row[4]
            run_attempts = report_row[5]
            check_attempts = report_row[6]
            report_data = [
                ["Метрика", "Значение"],
                ["Дата отчета", report_date_str],
                ["Всего попыток", total_attempts],
                ["Успешных попыток", successful_attempts],
                ["Процент успешных попыток", success_percentage],
                ["Уникальных пользователей", unique_users],
                ["Попыток типа 'run'", run_attempts],
                ["Попыток типа 'check'", check_attempts]
            ]
            report_values = {
                "report_date": report_date_str,
                "total_attempts": total_attempts,
                "successful_attempts": successful_attempts,
                "success_percentage": success_percentage,
                "unique_users": unique_users,
                "run_attempts": run_attempts,
                "check_attempts": check_attempts
            }

        gc = get_gspread_client(credentials_path)
        spreadsheet = gc.open(spreadsheet_name)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"Лист '{worksheet_name}' для отчета найден.")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=100, cols=10)
            print(f"Создан новый лист: '{worksheet_name}' для отчета.")

        worksheet.clear()
        worksheet.update(report_data, "A1")
        logging.info(f"Ежедневный отчет за {report_date_str} успешно загружен в Google Sheets: '{spreadsheet_name}' -> '{worksheet_name}'")
        print(f"Ежедневный отчет за {report_date_str} успешно загружен в Google Sheets: '{spreadsheet_name}' -> '{worksheet_name}'")

    except psycopg2.Error as e:
        logging.error(f"Ошибка БД при создании отчета ({type(e).__name__}): {e}")
        print(f"Ошибка БД при создании отчета: {e}")
    except Exception as e:
        logging.error(f"Ошибка при создании ежедневного отчета: {e}")
        print(f"Ошибка при создании ежедневного отчета: {e}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Соединение с БД для отчета закрыто.")
    
    return report_values

def send_email_notification(report_data):
    """
    Отправляет уведомление по электронной почте с агрегированными данными.
    """
    if not EMAIL_SENDER or not EMAIL_SENDER_PASSWORD or not EMAIL_RECEIVER:
        logging.warning("Настройки Email неполные. Проверьте .env файл.")
        print("Настройки Email неполные. Проверьте .env файл.")
        return

    msg = EmailMessage()
    msg['Subject'] = f"Ежедневный отчет по активности Grader за {report_data.get('report_date', 'сегодня')}"
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    email_body = f"""
    Привет!

    {"Нет данных за сегодняшний день." if report_data.get('total_attempts', 0) == 0 else "Вот краткий отчет по активности Grader:"}

    Дата: {report_data.get('report_date', 'сегодняшний день')}
    - Всего попыток: {report_data.get('total_attempts', 0)}
    - Успешных попыток: {report_data.get('successful_attempts', 0)}
    - Процент успешных попыток: {report_data.get('success_percentage', '0.00%')}
    - Уникальных пользователей: {report_data.get('unique_users', 0)}
    - Попыток типа 'run': {report_data.get('run_attempts', 0)}
    - Попыток типа 'check': {report_data.get('check_attempts', 0)}

    Полные сырые данные и детализированный отчет доступны в Google Sheets:
    {SPREADSHEET_URL}

    Скрипт успешно завершен.
    Хорошего дня!
    """
    msg.set_content(email_body)

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT, context=context) as server:
            server.login(EMAIL_SENDER, EMAIL_SENDER_PASSWORD)
            server.send_message(msg)
        logging.info(f"Уведомление по Email успешно отправлено на {EMAIL_RECEIVER}")
        print(f"Уведомление по Email успешно отправлено на {EMAIL_RECEIVER}")
    except Exception as e:
        logging.error(f"Ошибка при отправке Email-уведомления: {e}")
        print(f"Ошибка при отправке Email-уведомления: {e}")

def main():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        setup_logging()
        delete_old_logs()
        logging.info("Скрипт запущен.")
        print("Загрузка данных с API...")
        data = fetch_data()
        if data:
            print("Обработка данных...")
            processed_data = process_data(data)
            if processed_data:
                print("Запись данных в базу...")
                save_to_db(processed_data)
                upload_raw_data_to_sheets(processed_data)
            else:
                print("Нет обработанных данных для дальнейшей обработки.")
        else:
            print("Не удалось получить данные от API.")

        daily_report_metrics = generate_daily_report_and_send_to_sheets()
        if daily_report_metrics:
            send_email_notification(daily_report_metrics)
        else:
            logging.warning("Отчетные данные для Email не были получены, Email не будет отправлен.")
            print("Отчетные данные для Email не были получены, Email не будет отправлен.")

    except Exception as e:
        logging.exception(f"Критическая ошибка: {e}")
        print(f"Критическая ошибка: {e}")
    finally:
        logging.info("Скрипт завершён.")
        print("Скрипт завершён.")

if __name__ == "__main__":
    main()
