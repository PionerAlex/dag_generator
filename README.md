# Генератор DAG для Airflow

Утилита, которая автоматически создаёт DAG-файлы Airflow по папке с исходниками
аналитиков. Один DAG = одна папка с исходниками.

## Как это работает

1. Аналитик кладёт в `sources/<имя_dag>/` три вида файлов:
   - `config.yml` — расписание и описание
   - `*.py` — Python-скрипты (каждая функция → отдельная таска)
   - `*.sql` — SQL-скрипты (каждая команда → отдельная таска)
2. Запускается `generator.py`
3. В `dags/<имя_dag>.py` появляется готовый DAG
4. Airflow подхватывает его автоматически

## Установка

```bash
pip install pyyaml
```

## Запуск

```bash
python generator.py
```

Пример вывода:
```
Сгенерирован: dags/daily_report.py
```

## Формат исходников

### `config.yml`

```yaml
schedule: "0 6 * * *"          # cron-выражение
description: "Ежедневный отчёт"
owner: analytics
```

### Python-скрипт

Каждая публичная функция (`def name(...)`) превращается в отдельную таску
`PythonOperator`. Приватные функции (`_name`) игнорируются.

```python
def extract_orders(**kwargs):
    """Достаём заказы из базы."""
    print("Извлекаем заказы...")
    return 100
```

### SQL-скрипт

Команды разделяются точкой с запятой. Каждая команда → отдельная таска
`SQLExecuteQueryOperator`. Имя файла без расширения = `conn_id` в Airflow.

```sql
DROP TABLE IF EXISTS daily_sales;
CREATE TABLE daily_sales AS
SELECT DATE(order_date) AS day, SUM(amount) AS total
FROM orders
GROUP BY DATE(order_date);
```

## Правила генерации

| Что | Как обрабатывается |
|---|---|
| Папка `sources/<name>/` | Один DAG с `dag_id="<name>"` |
| Дата старта DAG | День запуска генератора |
| Расписание | Из `config.yml` (поле `schedule`) |
| Python-функция | Отдельная таска `PythonOperator` |
| SQL-команда | Отдельная таска `SQLExecuteQueryOperator` |
| Порядок тасок | Цепочка `>>` в порядке файлов и определений |
| Параметры подключения | В Airflow Connections, не в DAG |

## Параметры подключения

Параметры БД **не хранятся в DAG**. В DAG указывается только `conn_id`,
например `transform`. Реальные host/login/password лежат в Airflow:

```
Admin → Connections → transform
  Host:     pg_extra
  Schema:   extradb_user
  Login:    extradb_user
  Password: extradb_password
  Port:     5432
```

## Инфраструктура

- Airflow в Docker (см. `docker-compose.yml`)
- Postgres — метаданные Airflow
- Postgres (`pg_extra`, порт 5433 наружу) — рабочая база
- Redis — брокер Celery
- Папка `dags/` монтируется в контейнер Airflow

## Пример: `daily_report`

**Исходники:**

```
sources/daily_report/
├── config.yml
├── extract.py
└── transform.sql
```

**Что получилось:**

```
dags/daily_report.py
```

Внутри — 4 таски, соединённые цепочкой:

```
extract_orders → clean_orders → sql_task_1 → sql_task_2
```

## Структура проекта

```
project/
├── generator.py           # генератор DAG
├── docker-compose.yml     # Airflow + Postgres + Redis
├── .env                   # AIRFLOW_UID и прочее
├── README.md              # этот файл
├── dags/                  # сгенерированные DAG (сюда пишет генератор)
├── logs/                  # логи Airflow
├── config/                # конфиги Airflow
├── plugins/               # плагины Airflow
└── sources/               # исходники аналитиков
    └── daily_report/
        ├── config.yml
        ├── extract.py
        └── transform.sql
```

## Ограничения

- Один DAG = одна папка. Вложенные папки не обрабатываются.
- Порядок тасок — линейный (`>>`), без ветвлений.
- Комментарии в SQL удаляются перед парсингом.