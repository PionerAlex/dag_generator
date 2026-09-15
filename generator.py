import ast
import re
from datetime import datetime
from pathlib import Path
import yaml

SOURCES_DIR = "sources"
OUTPUT_DIR = "dags"          


def read_config(folder):
    config_path = folder / "config.yml"
    if not config_path.exists():
        return {
            "schedule": "@daily",
            "description": f"DAG {folder.name}",
            "owner": "airflow",
        }
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_functions(py_file):
    """Ищет все функции в python-файле."""
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = []

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            if node.name.startswith("_"):
                continue
            func_code = ast.get_source_segment(source, node)
            docstring = ast.get_docstring(node) or ""
            functions.append({
                "name": node.name,
                "code": func_code,
                "docstring": docstring,
            })
    return functions


def find_sql_commands(sql_file):
    """Делит SQL-файл на отдельные команды по точке с запятой."""
    text = sql_file.read_text(encoding="utf-8")
    text = re.sub(r"--.*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    commands = []
    for part in text.split(";"):
        part = part.strip()
        if part:
            commands.append(part)
    return commands


def indent(text, spaces):
    pad = " " * spaces
    return "\n".join(pad + line for line in text.splitlines())


def make_python_task(func):
    task_id = func["name"]
    code = f'''
    def wrapper_{task_id}(**kwargs):
        """{func["docstring"]}"""
{indent(func["code"], 8)}

    task_{task_id} = PythonOperator(
        task_id="{task_id}",
        python_callable=wrapper_{task_id},
    )
'''
    return code


def make_sql_task(command, index, conn_id):
    task_id = f"sql_task_{index}"
    safe_sql = command.replace('"""', '\\"\\"\\"')
    code = f'''
    task_{task_id} = SQLExecuteQueryOperator(
        task_id="{task_id}",
        conn_id="{conn_id}",
        sql="""{safe_sql}""",
    )
'''
    return code


def generate_dag(folder, output_dir):
    dag_name = folder.name
    config = read_config(folder)

    today = datetime.now()
    start_date = f"datetime({today.year}, {today.month}, {today.day})"

    task_codes = []
    task_ids = []
    counter = 0

    for py_file in sorted(folder.glob("*.py")):
        for func in find_functions(py_file):
            task_codes.append(make_python_task(func))
            task_ids.append(f"task_{func['name']}")

    for sql_file in sorted(folder.glob("*.sql")):
        conn_id = sql_file.stem   # имя файла без расширения = conn_id в Airflow
        for i, cmd in enumerate(find_sql_commands(sql_file), start=1):
            counter += 1
            task_codes.append(make_sql_task(cmd, counter, conn_id))
            task_ids.append(f"task_sql_task_{counter}")

    chain = " >> ".join(task_ids) if len(task_ids) > 1 else ""

    dag_code = f'''


from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="{dag_name}",
    description="{config.get("description", "")}",
    schedule="{config.get("schedule", "@daily")}",
    start_date={start_date},
    catchup=False,
    tags=["generated"],
) as dag:
{"".join(task_codes)}
    # зависимости
    {chain}

'''

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{dag_name}.py"
    out_file.write_text(dag_code, encoding="utf-8")
    print(f"Сгенерирован: {out_file}")


def main():
    sources = Path(SOURCES_DIR)
    output = Path(OUTPUT_DIR)

    for folder in sources.iterdir():
        if folder.is_dir():
            generate_dag(folder, output)


if __name__ == "__main__":
    main()