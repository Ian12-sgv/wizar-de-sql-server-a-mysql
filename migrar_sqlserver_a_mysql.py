#!/usr/bin/env python3
import getpass
import sys
from typing import Dict, List, Optional

import pyodbc
import pymysql


# ---------------------------
# Utilidades de consola
# ---------------------------
def ask(prompt: str, default: Optional[str] = None, secret: bool = False) -> str:
    if default:
        prompt = f"{prompt} [{default}]: "
    else:
        prompt = f"{prompt}: "
    return (getpass.getpass(prompt) if secret else input(prompt)).strip() or (default or "")

def confirm(prompt: str, default: bool = True) -> bool:
    d = "S/n" if default else "s/N"
    ans = input(f"{prompt} ({d}): ").strip().lower()
    if not ans:
        return default
    return ans in ("s", "si", "sí", "y", "yes")

def choose(prompt: str, options: List[str]) -> str:
    print(prompt)
    for i, opt in enumerate(options, 1):
        print(f"  {i}) {opt}")
    while True:
        sel = input("Elige opción: ").strip()
        if sel.isdigit() and 1 <= int(sel) <= len(options):
            return options[int(sel) - 1]
        print("Opción inválida.")

def choose_columns_interactive(all_cols: List[str]) -> List[str]:
    print("\nColumnas disponibles:")
    for i, c in enumerate(all_cols, 1):
        print(f"  {i}) {c}")
    print("Escribe una lista separada por comas (ej: id,codigo,descripcion).")
    print("Deja vacío para migrar TODAS.")
    sel = input("Columnas a migrar: ").strip()
    if not sel:
        return all_cols
    chosen = [c.strip() for c in sel.split(",") if c.strip()]
    # validar
    missing = [c for c in chosen if c not in all_cols]
    if missing:
        print("Columnas no encontradas:", ", ".join(missing))
        sys.exit(1)
    return chosen


# ---------------------------
# Conexiones
# ---------------------------
def connect_sqlserver(dsn: Optional[str], server: Optional[str], database: str,
                      user: Optional[str], password: Optional[str], driver: str) -> pyodbc.Connection:
    """
    Admite DSN o cadena sin DSN (con Driver).
    """
    if dsn:
        conn_str = f"DSN={dsn};DATABASE={database};"
        if user:
            conn_str += f"UID={user};PWD={password};"
        else:
            conn_str += "Trusted_Connection=Yes;"
    else:
        # Driver recomendado: ODBC Driver 17/18 for SQL Server (también sirve 13 si lo tienes)
        # Ajusta Encrypt/TrustServerCertificate según tu entorno
        auth = f"UID={user};PWD={password};" if user else "Trusted_Connection=Yes;"
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"{auth}Encrypt=No;TrustServerCertificate=Yes;"
        )
    return pyodbc.connect(conn_str, autocommit=False)


def connect_mysql(host: str, port: int, database: Optional[str],
                  user: str, password: str, charset: str = "utf8mb4") -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host, port=port, user=user, password=password,
        database=database, charset=charset, autocommit=False
    )


# ---------------------------
# Metadatos y mapeos
# ---------------------------
TYPE_MAP = {
    # numéricos
    "bigint": "BIGINT",
    "int": "INT",
    "smallint": "SMALLINT",
    "tinyint": "TINYINT",
    "bit": "TINYINT(1)",
    "decimal": "DECIMAL",     # requiere (p,s)
    "numeric": "DECIMAL",     # requiere (p,s)
    "money": "DECIMAL(19,4)",
    "smallmoney": "DECIMAL(10,4)",
    "float": "DOUBLE",
    "real": "FLOAT",

    # textos
    "varchar": "VARCHAR",     # requiere (len)
    "nvarchar": "VARCHAR",    # mapeamos a utf8mb4
    "char": "CHAR",
    "nchar": "CHAR",
    "text": "LONGTEXT",
    "ntext": "LONGTEXT",

    # fechas
    "date": "DATE",
    "datetime": "DATETIME",
    "datetime2": "DATETIME",
    "smalldatetime": "DATETIME",
    "time": "TIME",
    "datetimeoffset": "DATETIME",  # pierde offset

    # binarios/otros
    "binary": "BINARY",
    "varbinary": "VARBINARY",
    "image": "LONGBLOB",
    "uniqueidentifier": "CHAR(36)",
    "xml": "LONGTEXT",
    "sql_variant": "JSON",  # aproximación
}

def fetch_tables_sqlserver(cur, schema: str) -> List[str]:
    cur.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA=?
        ORDER BY TABLE_NAME
    """, (schema,))
    return [r[0] for r in cur.fetchall()]

def fetch_columns_sqlserver(cur, schema: str, table: str):
    cur.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
               COLUMNPROPERTY(object_id(QUOTENAME(c.TABLE_SCHEMA)+'.'+QUOTENAME(c.TABLE_NAME)),
                              c.COLUMN_NAME,'IsIdentity') AS IS_IDENTITY,
               c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=?
        ORDER BY c.ORDINAL_POSITION
    """, (schema, table))
    cols = cur.fetchall()
    return [
        {
            "name": x[0],
            "type": x[1].lower(),
            "char_len": x[2],
            "num_prec": x[3],
            "num_scale": x[4],
            "nullable": (x[5].upper() == "YES"),
            "is_identity": (x[6] == 1),
            "default": x[7],
        } for x in cols
    ]

def fetch_primary_key_sqlserver(cur, schema: str, table: str) -> List[str]:
    cur.execute("""
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.TABLE_SCHEMA=? AND tc.TABLE_NAME=? AND tc.CONSTRAINT_TYPE='PRIMARY KEY'
        ORDER BY kcu.ORDINAL_POSITION
    """, (schema, table))
    return [r[0] for r in cur.fetchall()]

def sqlserver_to_mysql_type(col: Dict) -> str:
    t = col["type"]
    mapped = TYPE_MAP.get(t, "TEXT")
    if mapped in ("VARCHAR", "CHAR", "VARBINARY", "BINARY"):
        length = col["char_len"] if col["char_len"] and col["char_len"] > 0 else 255
        return f"{mapped}({length})"
    if mapped == "DECIMAL":
        p = col["num_prec"] or 18
        s = col["num_scale"] or 0
        return f"DECIMAL({p},{s})"
    return mapped

def build_create_table_mysql(table: str, columns: List[Dict], pk_cols: List[str]) -> str:
    parts = []
    for col in columns:
        col_def = f"`{col['name']}` {sqlserver_to_mysql_type(col)}"
        if col["is_identity"]:
            col_def += " AUTO_INCREMENT"
        if not col["nullable"] or col["is_identity"]:
            col_def += " NOT NULL"
        else:
            col_def += " NULL"
        if col["default"]:
            d = str(col["default"]).strip()
            d = d.strip("()")
            if d.lower() in ("getdate", "getdate()", "sysdatetime()", "current_timestamp"):
                col_def += " DEFAULT CURRENT_TIMESTAMP"
            elif d.lower() in ("newid", "newid()"):
                pass
            else:
                try:
                    float(d)
                    col_def += f" DEFAULT {d}"
                except Exception:
                    d = d.lstrip("N").strip("'").replace("\\", "\\\\").replace("'", "\\'")
                    col_def += f" DEFAULT '{d}'"
        parts.append(col_def)
    if pk_cols:
        parts.append(f"PRIMARY KEY ({', '.join('`'+c+'`' for c in pk_cols)})")
    cols_sql = ",\n  ".join(parts)
    return f"CREATE TABLE IF NOT EXISTS `{table}` (\n  {cols_sql}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

def quote_ident_list(cols: List[str]) -> str:
    return ", ".join(f"`{c}`" for c in cols)


# ---------------------------
# Migración de datos
# ---------------------------
def copy_table(sql_cur, mysql_conn, mysql_cur, schema: str, table: str, batch_size: int = 10_000,
               only_cols: Optional[List[str]] = None):
    print(f"\n[+] Preparando migración de {schema}.{table} ...")

    all_cols_meta = fetch_columns_sqlserver(sql_cur, schema, table)
    pk = fetch_primary_key_sqlserver(sql_cur, schema, table)

    # Filtrar columnas si se especifica subconjunto
    if only_cols:
        cols = [c for c in all_cols_meta if c["name"] in only_cols]
        pk = [c for c in pk if c in only_cols]
    else:
        cols = all_cols_meta

    # Crear tabla destino (solo con columnas elegidas)
    create_sql = build_create_table_mysql(table, cols, pk)
    print(f"[+] Creando tabla en MySQL si no existe...")
    mysql_cur.execute(create_sql)
    mysql_conn.commit()

    col_names = [c["name"] for c in cols]
    select_cols = ", ".join(f"[{c}]" for c in col_names)
    order_by = f" ORDER BY {', '.join('['+c+']' for c in pk)}" if pk else ""
    sql_cur.execute(f"SELECT {select_cols} FROM [{schema}].[{table}]{order_by};")

    # Preparar INSERT
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_sql = f"INSERT INTO `{table}` ({quote_ident_list(col_names)}) VALUES ({placeholders})"

    total = 0
    print(f"[+] Copiando datos por lotes de {batch_size} filas...")
    while True:
        rows = sql_cur.fetchmany(batch_size)
        if not rows:
            break
        data = [tuple(row) for row in rows]
        mysql_cur.executemany(insert_sql, data)
        mysql_conn.commit()
        total += len(data)
        print(f"    - {total} filas migradas...")

    print(f"[✓] Tabla {schema}.{table} migrada con {total} filas (columnas: {', '.join(col_names)}).")


# ---------------------------
# Flujo principal (Wizard)
# ---------------------------
def main():
    print("\n=== Wizard: Migrar de SQL Server a MySQL ===\n")

    # 1) ¿Migrar toda la base de datos?
    migra_db_completa = confirm("¿Deseas migrar toda la base de datos?", True)

    print("\n--- Conexión a SQL Server ---")
    use_dsn = confirm("¿Usarás un DSN de ODBC configurado?", False)
    dsn = ask("Nombre del DSN") if use_dsn else None
    driver = "ODBC Driver 17 for SQL Server"
    if not use_dsn:
        driver = ask("Nombre del Driver ODBC (exacto, según tu instalación)", driver or "ODBC Driver 17 for SQL Server")
    server = None if use_dsn else ask("Servidor (host\\instancia o host,puerto)", "localhost")
    sql_db = ask("Base de datos origen (SQL Server)")
    sql_user = ask("Usuario (vacío para autenticación integrada)", "")
    sql_pass = ask("Contraseña", secret=True) if sql_user else None
    schema = ask("Esquema (SQL Server)", "dbo")

    try:
        sql_cnx = connect_sqlserver(dsn, server, sql_db, sql_user or None, sql_pass, driver)
        sql_cur = sql_cnx.cursor()
        print("[✓] Conectado a SQL Server.")
    except Exception as e:
        print("Error conectando a SQL Server:", e)
        sys.exit(1)

    print("\n--- Conexión a MySQL ---")
    mysql_host = ask("Host MySQL", "localhost")
    mysql_port = int(ask("Puerto MySQL", "3306"))
    mysql_user = ask("Usuario MySQL", "root")
    mysql_pass = ask("Contraseña MySQL", secret=True)
    mysql_db = ask("Nombre de la base de datos destino (se creará si no existe)")

    try:
        # Conectar sin DB para poder crearla si no existe
        mysql_cnx = connect_mysql(mysql_host, mysql_port, None, mysql_user, mysql_pass)
        mysql_cur = mysql_cnx.cursor()
        mysql_cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` DEFAULT CHARSET utf8mb4;")
        mysql_cnx.commit()
        mysql_cnx.select_db(mysql_db)
        print("[✓] Conectado a MySQL y base preparada.")
    except Exception as e:
        print("Error conectando a MySQL:", e)
        sys.exit(1)

    try:
        # 2) Flujos según selección
        if migra_db_completa:
            # Migrar TODAS las tablas con TODAS las columnas
            tables = fetch_tables_sqlserver(sql_cur, schema)
            if not tables:
                print(f"No se encontraron tablas en {sql_db}.{schema}.")
                sys.exit(0)
            print(f"\nSe migrarán {len(tables)} tablas del esquema {schema}:")
            for t in tables:
                print(" -", t)
            if not confirm("¿Confirmas migrar todas las tablas con todas sus columnas?"):
                print("Cancelado por el usuario.")
                sys.exit(0)
            for t in tables:
                copy_table(sql_cur, mysql_cnx, mysql_cur, schema, t)

        else:
            # No base completa -> ¿todas las tablas o una sola?
            choice = choose("\n¿Deseas migrar todas las tablas del esquema o una sola tabla?",
                            ["Todas las tablas", "Una sola tabla"])

            if choice == "Todas las tablas":
                tables = fetch_tables_sqlserver(sql_cur, schema)
                if not tables:
                    print(f"No se encontraron tablas en {sql_db}.{schema}.")
                    sys.exit(0)
                print(f"\nSe migrarán {len(tables)} tablas del esquema {schema} (todas las columnas).")
                if not confirm("¿Confirmas migrar todas las tablas con todas sus columnas?"):
                    print("Cancelado por el usuario.")
                    sys.exit(0)
                for t in tables:
                    copy_table(sql_cur, mysql_cnx, mysql_cur, schema, t)

            else:
                # Una sola tabla -> elegir tabla
                tables = fetch_tables_sqlserver(sql_cur, schema)
                if not tables:
                    print(f"No se encontraron tablas en {sql_db}.{schema}.")
                    sys.exit(0)
                print("\nTablas disponibles:")
                for i, t in enumerate(tables, 1):
                    print(f"  {i}) {t}")
                while True:
                    sel = input("Escribe el nombre exacto de la tabla a migrar: ").strip()
                    if sel in tables:
                        table = sel
                        break
                    print("Tabla no encontrada. Intenta de nuevo.")

                # 3) Elegir columnas: todas o algunas
                col_choice = choose("\n¿Deseas migrar todas las columnas o solo algunas?",
                                    ["Todas las columnas", "Algunas columnas"])

                if col_choice == "Todas las columnas":
                    copy_table(sql_cur, mysql_cnx, mysql_cur, schema, table)
                else:
                    all_cols = [c["name"] for c in fetch_columns_sqlserver(sql_cur, schema, table)]
                    only_cols = choose_columns_interactive(all_cols)

                    # Aviso por NOT NULL sin default si se excluyen
                    meta = fetch_columns_sqlserver(sql_cur, schema, table)
                    meta_by = {m["name"]: m for m in meta}
                    excluded = [c for c in all_cols if c not in only_cols]
                    risky = [c for c in excluded if (not meta_by[c]["nullable"]) and (meta_by[c]["default"] is None)]
                    if risky:
                        print("\n[!] Aviso: Estás excluyendo columnas NOT NULL sin DEFAULT:")
                        for c in risky:
                            print("   -", c)
                        print("Esto podría causar errores si en el destino esas columnas se requieren.")
                        if not confirm("¿Continuar de todas formas?", False):
                            print("Cancelado por el usuario.")
                            sys.exit(0)

                    copy_table(sql_cur, mysql_cnx, mysql_cur, schema, table, only_cols=only_cols)

        print("\n[🎉] Migración finalizada con éxito.")
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print("\n[!] Error durante la migración:", e)
        try:
            mysql_cnx.rollback()
        except Exception:
            pass
    finally:
        try:
            sql_cur.close(); sql_cnx.close()
        except Exception:
            pass
        try:
            mysql_cur.close(); mysql_cnx.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
