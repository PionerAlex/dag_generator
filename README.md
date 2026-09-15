# Генератор DAG для Airflow

Утилита, которая автоматически создаёт DAG-файлы Airflow по папке с исходниками.
Один DAG = одна папка с исходниками.

## Что делает

1. Сканирует папку `sources/` — каждая подпапка = один DAG
2. Парсит Python-скрипты: каждая функция → отдельная таска `PythonOperator`
3. Парсит SQL-скрипты: каждая команда (по `;`) → отдельная таска `SQLExecuteQueryOperator`
4. Читает расписание и описание из `config.yml` в каждой папке
5. Кладёт готовые DAG-файлы в `dags/` — Airflow их подхватывает автоматически

## Зависимости

- Python 3.10+
- `pyyaml` — единственная внешняя библиотека

```bash
pip install pyyaml
```

Всё остальное — стандартная библиотека: `ast` (парсинг Python), `re` (парсинг SQL),
`datetime`, `pathlib`.

## Запуск

Из корня проекта:

```bash
python generator.py
```

Пример вывода:

```
Сгенерирован: dags/daily_report.py
Сгенерирован: dags/init_db.py
Сгенерирован: dags/ml_pipeline.py
Сгенерирован: dags/weekly_cleanup.py
```

После генерации перезапустить Airflow:

```bash
docker compose restart airflow-scheduler airflow-webserver airflow-worker
```

## Структура проекта

```
dag-generator/
├── generator.py              # генератор DAG
├── docker-compose.yml        # Airflow + Postgres + Redis
├── .env                      # AIRFLOW_UID и прочее
├── README.md                 # этот файл
├── dags/                     # сюда пишет генератор
│   ├── daily_report.py
│   ├── init_db.py
│   ├── ml_pipeline.py
│   └── weekly_cleanup.py
└── sources/                  # исходники аналитиков
    ├── init_db/
    │   ├── config.yml
    │   └── transform.sql
    ├── daily_report/
    │   ├── config.yml
    │   ├── extract.py
    │   └── transform.sql
    ├── ml_pipeline/
    │   ├── config.yml
    │   └── train.py
    └── weekly_cleanup/
        ├── config.yml
        └── transform.sql
```

## Формат исходников

### `config.yml`

```yaml
schedule: "0 6 * * *" # cron-выражение или @once / @daily
description: "Ежедневный отчёт"
owner: analytics
```

### Python-скрипт

Каждая публичная функция (`def name(...)`) → отдельная таска.
Приватные функции (`_name`) игнорируются.

```python
def extract_orders(**kwargs):
    """Достаём заказы из базы."""
    print("Извлекаем заказы...")
    return 100
```

### SQL-скрипт

Команды разделяются точкой с запятой. Каждая команда → отдельная таска.
**Имя файла без расширения = `conn_id` в Airflow.**

```sql
DROP TABLE IF EXISTS daily_sales;
CREATE TABLE daily_sales AS
SELECT DATE(order_date) AS day, SUM(amount) AS total
FROM orders
GROUP BY DATE(order_date);
```

Для файла `transform.sql` используется connection `transform`.
Один connection можно использовать для нескольких DAG-ов — просто называй
SQL-файлы одинаково (`transform.sql`) в разных папках.

## Правила генерации

| Что                     | Как обрабатывается                                   |
| ----------------------- | ---------------------------------------------------- |
| Папка `sources/<name>/` | Один DAG с `dag_id="<name>"`                         |
| Дата старта DAG         | День запуска генератора                              |
| Расписание              | Из `config.yml` (поле `schedule`)                    |
| Python-функция          | Отдельная таска `PythonOperator`                     |
| SQL-команда             | Отдельная таска `SQLExecuteQueryOperator`            |
| Порядок тасок           | Линейная цепочка `>>` в порядке файлов и определений |
| Параметры подключения   | В Airflow Connections, не в DAG                      |
| `conn_id`               | Имя SQL-файла без расширения                         |

## Созданные DAG-и

| DAG              | Расписание  | Что делает                                   |
| ---------------- | ----------- | -------------------------------------------- |
| `init_db`        | `@once`     | Создаёт таблицы `orders`, `logs`, `sessions` |
| `daily_report`   | `0 6 * * *` | Считает `daily_sales` из `orders`            |
| `ml_pipeline`    | `0 3 * * *` | 3 Python-таски: load → train → save          |
| `weekly_cleanup` | `0 2 * * 0` | Чистит старые записи из `logs` и `sessions`  |

## Параметры подключения

Параметры БД **не хранятся в DAG**. В DAG указывается только `conn_id`.
Реальные host/login/password лежат в Airflow Connections.

Создать connection через CLI:

```bash
docker compose exec airflow-webserver airflow connections add 'transform' \
    --conn-type 'postgres' \
    --conn-host 'pg_extra' \
    --conn-schema 'extradb_user' \
    --conn-login 'extradb_user' \
    --conn-password 'extradb_password' \
    --conn-port 5432
```

Или через UI: `Admin → Connections → +`.

**Важно:** host — `pg_extra` (имя сервиса в docker-сети), порт — `5432`.
Порт `5433` — только для доступа с хоста (DBeaver), изнутри Docker он не работает.

## Инфраструктура

- Airflow в Docker (см. `docker-compose.yml`)
- Postgres — метаданные Airflow
- Postgres (`pg_extra`, порт 5433 наружу) — рабочая база
- Redis — брокер Celery
- Папка `dags/` монтируется в контейнер Airflow

## Порядок первого запуска

1. `pip install pyyaml`
2. `python generator.py` — создать DAG-и
3. `docker compose up -d` — поднять Airflow
4. Создать connection `transform` в Airflow
5. В UI: `init_db` → Trigger (создаст таблицы)
6. `daily_report`, `ml_pipeline`, `weekly_cleanup` → Trigger

## Ограничения

- Один DAG = одна папка. Вложенные папки не обрабатываются.
- Порядок тасок линейный (`>>`), без ветвлений.
- Комментарии в SQL (`--` и `/* */`) удаляются перед парсингом.
- Параметры connection не читаются из `config.yml` — берутся из имени файла.
