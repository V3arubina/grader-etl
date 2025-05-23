# Grader ETL

Grader ETL — это скрипт для автоматического сбора, обработки, хранения и визуализации данных из системы Grader.  
Проект предназначен для анализа активности пользователей, формирования ежедневных отчетов и их отправки на почту, а также для сохранения данных в Google Sheets и базе данных PostgreSQL.  
Подойдет как для учебных, так и для рабочих задач, связанных с образовательными платформами.

---

## Начало работы

Данные инструкции помогут вам развернуть проект на вашей локальной машине для целей разработки и тестирования.

### Необходимые условия

- Python >= 3.8
- PostgreSQL (желательно версии 12+)
- Google Service Account credentials (файл доступа для работы с Google Sheets)
- pip (пакетный менеджер Python)

### Установка

#### 1. Клонирование репозитория

git clone git@github.com/V3arubina/grader-etl.git
cd grader-etl

#### 2. Установка зависимостей

Убедитесь, что у вас установлен pip и выполните:

pip install -r requirements.txt

#### 3. Настройка окружения

Создайте файл `.env` в корне проекта (или скопируйте и переименуйте `.env.example`) и заполните его следующими данными:

DB_NAME=ваше_название_базы
DB_USER=имя_пользователя
DB_PASSWORD=пароль
DB_HOST=localhost
DB_PORT=5432

# Настройки для отправки email
EMAIL_SENDER=your_email@gmail.com
EMAIL_SENDER_PASSWORD=your_email_password # Используйте App Password, если у вас 2FA
EMAIL_RECEIVER=receiver_email@gmail.com

#### 4. Добавьте файл с Google credentials

Для работы с Google Sheets API вам потребуется файл учетных данных сервисного аккаунта (Service Account).

- Сгенерируйте и скачайте JSON-файл ключа из Google Cloud Console.
- Переименуйте скачанный файл в your_credentials.json.
- Поместите этот файл в папку credentials/ в корне вашего проекта.

Важно: Папка credentials/ (вместе с your_credentials.json) игнорируется Git'ом для обеспечения безопасности.

---

## Использование

Для запуска основного скрипта используйте команду:

python main.py

Данные будут загружены из API, сохранены в базу данных, выгружены в Google Sheets и отправлен отчет на почту.

---

## Тестирование

В проекте нет автоматических тестов. Для проверки функциональности рекомендуем:

- Проверить успешную запись данных в базу данных PostgreSQL.
- Проверить обновление данных в Google Sheets.
- Проверить получение email-отчёта.

---

## Авторы

- [V3arubina](https://github.com/V3arubina)

---
