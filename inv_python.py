import os
import csv
import ast
import datetime


def analyze_python_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    tree = ast.parse(content)

    # Extraer información
    db_info = extract_db_info(tree)
    tables = extract_tables(tree)
    bigquery_project = extract_bigquery_project(tree)

    return {
        'file_name': os.path.basename(file_path),
        'file_path': file_path,
        'creation_date': datetime.datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d'),
        'function': extract_function(tree),
        'database': db_info.get('database', ''),
        'tables': ', '.join(tables),
        'parameters': extract_parameters(tree),
        'bigquery_project': bigquery_project
    }


def extract_function(tree):
    # Esta es una implementación simplificada. Podrías mejorarla para obtener una descripción más precisa.
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            return f"Contains function: {node.name}"
    return "Main script"


def extract_db_info(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            keys = [k.s for k in node.keys if isinstance(k, ast.Str)]
            if 'host' in keys and 'database' in keys:
                return {k.s: v.s for k, v in zip(node.keys, node.values) if isinstance(k, ast.Str) and isinstance(v, ast.Str)}
    return {}


def extract_tables(tree):
    tables = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Str) and 'FROM' in node.s:
            parts = node.s.split()
            if 'FROM' in parts:
                table_index = parts.index('FROM') + 1
                if table_index < len(parts):
                    tables.add(parts[table_index].strip('"`'))
    return list(tables)


def extract_parameters(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
            if 'sys' in [n.name for n in node.names]:
                return "Uses command-line parameters"
    return "No command-line parameters"


def extract_bigquery_project(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == 'project_id':
                    if isinstance(node.value, ast.Str):
                        return node.value.s
    return ''


def inventory_python_files(directory):
    inventory = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    inventory.append(analyze_python_file(file_path))
                except Exception as e:
                    print(f"Error analyzing {file_path}: {str(e)}")
    return inventory


def save_to_csv(inventory, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['file_name', 'file_path', 'creation_date', 'function',
                      'database', 'tables', 'parameters', 'bigquery_project']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in inventory:
            writer.writerow(item)


if __name__ == "__main__":
    # Reemplaza esto con la ruta a tu directorio
    directory = r"C:\Path\To\Your\Python\Files"
    output_file = "python_files_inventory.csv"

    inventory = inventory_python_files(directory)
    save_to_csv(inventory, output_file)
    print(f"Inventario guardado en {output_file}")
