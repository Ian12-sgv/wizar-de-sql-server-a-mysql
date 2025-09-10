#!/usr/bin/env python3
# actualizar.py
import getpass
import sys
from typing import Dict, List, Optional

import pyodbc
import pymysql


# =========================
# Utilidades CLI
# =========================
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
    print("\nColumnas disponibles para ACTUALIZAR:")
    for i, c in enumerate(all_cols, 1):
        print(f"  {i}) {c}")
    print("Escribe una lista separada por comas (ej: codigo,descripcion,precio).")
    print("Deja vacío para seleccionar TODAS (excepto PK/rowversion).")
    sel = input("Columnas a actualizar: ").strip()
    if not sel:
        return all_cols
    chosen = [c.strip() for c in sel.split(",") if c.strip()]
    missing = [c for c in chosen if c not in all_cols]
    if missing:
        print("Columnas no encontradas:", ", ".join(missing))
        sys.exit(1)
    return chosen


# =========================
# Conexiones
# =========================
def connect_sqlserver(dsn: Optional[str], server: Optional[str], database: str,
                      user: Optional[str], password: Optional[str], driver: str) -> pyodbc.Connection:
    if dsn:
        conn_str = f"DSN={dsn};DATABASE={database};"
        if user:
            conn_str += f"UID={user};PWD={password};"
        else:
            conn_str += "Trusted_Connection=Yes;"
    else:
        auth = f"UID={user};PWD={password};" if user else "Trusted_Connection=Yes;"
        conn_str = (
            f"DRIVER={{{driver}}});SERVER={server};DATABASE={database};"
            f"{auth}Encrypt=No;TrustServerCertificate=Yes;"
        )
        # Nota: hay una llave extra en DRIVER arriba, la corregimos:
        conn_str = conn_str.replace("}})", "}};")
    return pyodbc.connect(conn_str, autocommit=False)

def connect_mysql(host: str, port: int, database: Optional[str],
                  user: str, password: str, charset: str = "utf8mb4") -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host, port=port, user=user, password=password,
        database=database, charset=charset, autocommit=False
    )


# =========================
# Metadatos
# =========================
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
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
               COLUMNPROPERTY(object_id(QUOTENAME(c.TABLE_SCHEMA)+'.'+QUOTENAME(c.TABLE_NAME)),
                              c.COLUMN_NAME,'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=?
        ORDER BY c.ORDINAL_POSITION
    """, (schema, table))
    rows = cur.fetchall()
    return [
        {
            "name": x[0],
            "type": x[1].lower(),
            "nullable": (x[2].upper() == "YES"),
            "is_identity": (x[3] == 1),
        } for x in rows
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

def fetch_mysql_pk_columns(mysql_cur, db: str, table: str) -> List[str]:
    mysql_cur.execute("""
        SELECT k.COLUMN_NAME
        FROM information_schema.table_constraints t
        JOIN information_schema.key_column_usage k
          ON t.constraint_name = k.constraint_name
         AND t.table_schema = k.table_schema
         AND t.table_name   = k.table_name
        WHERE t.table_schema=%s AND t.table_name=%s AND t.constraint_type='PRIMARY KEY'
        ORDER BY k.ORDINAL_POSITION
    """, (db, table))
    return [r[0] for r in mysql_cur.fetchall()]


# =========================
# Lógica de actualización
# =========================
def update_only(sql_cur, mysql_conn, mysql_cur, schema: str, table: str,
                pk_cols: List[str], upd_cols: List[str], batch_size: int = 10_000,
                where_clause: Optional[str] = None):
    if not pk_cols:
        print("[!] No hay clave primaria. UPDATE-only requiere columnas clave para el WHERE.")
        sys.exit(1)

    # No permitimos actualizar columnas PK
    upd_cols = [c for c in upd_cols if c not in pk_cols]
    if not upd_cols:
        print("[!] No hay columnas para actualizar (todas eran PK).")
        return

    select_cols = ", ".join(f"[{c}]" for c in (pk_cols + upd_cols))
    where_sql = f" WHERE {where_clause} " if where_clause else ""
    order_by = f" ORDER BY {', '.join('['+c+']' for c in pk_cols)}"
    sql_cur.execute(f"SELECT {select_cols} FROM [{schema}].[{table}]{where_sql}{order_by};")

    set_clause = ", ".join(f"`{c}`=%s" for c in upd_cols)
    where_pred = " AND ".join(f"`{c}`=%s" for c in pk_cols)
    upd_sql = f"UPDATE `{table}` SET {set_clause} WHERE {where_pred}"

    total = 0
    while True:
        rows = sql_cur.fetchmany(batch_size)
        if not rows:
            break
        # Reordena: [upd_vals..., pk_vals...]
        data = []
        for r in rows:
            # r = (pk1, pk2, ..., col1, col2, ...)
            pk_vals = tuple(r[:len(pk_cols)])
            col_vals = tuple(r[len(pk_cols):])
            data.append(tuple(list(col_vals) + list(pk_vals)))
        mysql_cur.executemany(upd_sql, data)
        mysql_conn.commit()
        total += len(data)
        print(f"  - {total} filas procesadas...")
    print(f"[✓] UPDATE completado. Filas consideradas: {total}.")

def upsert_mode(sql_cur, mysql_conn, mysql_cur, schema: str, table: str,
                pk_cols: List[str], upd_cols: List[str], batch_size: int = 10_000,
                where_clause: Optional[str] = None):
    """
    UPSERT: Inserta si no existe (por PK) y actualiza columnas seleccionadas si existe.
    Requiere que la tabla MySQL tenga PRIMARY KEY/UNIQUE en pk_cols.
    """
    if not pk_cols:
        print("[!] No hay clave primaria. UPSERT requiere columnas clave.")
        sys.exit(1)

    # En upsert, enviamos PK + columnas a actualizar (sin duplicar PK en update list)
    non_pk_upd = [c for c in upd_cols if c not in pk_cols]
    insert_cols = pk_cols + non_pk_upd

    if not insert_cols:
        print("[!] No hay columnas para insertar/actualizar.")
        return

    select_cols = ", ".join(f"[{c}]" for c in insert_cols)
    where_sql = f" WHERE {where_clause} " if where_clause else ""
    order_by = f" ORDER BY {', '.join('['+c+']' for c in pk_cols)}"
    sql_cur.execute(f"SELECT {select_cols} FROM [{schema}].[{table}]{where_sql}{order_by};")

    placeholders = ", ".join(["%s"] * len(insert_cols))
    update_set = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in non_pk_upd) or ""
    if update_set:
        sql = f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in insert_cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_set}"
    else:
        # Si no hay columnas para actualizar (caso raro), hacemos insert ignorando duplicados
        sql = f"INSERT IGNORE INTO `{table}` ({', '.join(f'`{c}`' for c in insert_cols)}) VALUES ({placeholders})"

    total = 0
    while True:
        rows = sql_cur.fetchmany(batch_size)
        if not rows:
            break
        data = [tuple(row) for row in rows]
        mysql_cur.executemany(sql, data)
        mysql_conn.commit()
        total += len(data)
        print(f"  - {total} filas procesadas...")
    print(f"[✓] UPSERT completado. Filas consideradas: {total}.")


# =========================
# Main
# =========================
def main():
    print("\n=== Actualizar MySQL desde SQL Server (solo columnas seleccionadas) ===\n")

    # Modo: UPDATE vs UPSERT
    mode = choose("Elige el modo de operación:", ["UPDATE (solo actualiza existentes)", "UPSERT (inserta o actualiza)"])

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
    mysql_db = ask("Base de datos destino (MySQL)")
    mysql_table = ask("Tabla destino (MySQL) (debe existir)")

    try:
        mysql_cnx = connect_mysql(mysql_host, mysql_port, mysql_db, mysql_user, mysql_pass)
        mysql_cur = mysql_cnx.cursor()
        print("[✓] Conectado a MySQL.")
    except Exception as e:
        print("Error conectando a MySQL:", e)
        sys.exit(1)

    # Elegir tabla origen (mismo nombre por defecto)
    tables = fetch_tables_sqlserver(sql_cur, schema)
    if not tables:
        print(f"No se encontraron tablas en {sql_db}.{schema}.")
        sys.exit(0)

    print("\nTablas disponibles en SQL Server:")
    for i, t in enumerate(tables, 1):
        print(f"  {i}) {t}")
    src_table = ask("Tabla origen (SQL Server)", mysql_table if mysql_table in tables else "" )
    if not src_table or src_table not in tables:
        print("Tabla origen no válida.")
        sys.exit(1)

    # Columnas
    meta = fetch_columns_sqlserver(sql_cur, schema, src_table)
    pk_sql = fetch_primary_key_sqlserver(sql_cur, schema, src_table)

    # Evitar proponer rowversion/timestamp o columnas identidad para UPDATE
    safe_cols = []
    for c in meta:
        if c["is_identity"]:
            continue
        if c["type"] in ("timestamp", "rowversion"):
            continue
        safe_cols.append(c["name"])

    print("\nClave primaria detectada en SQL Server:", pk_sql or "(ninguna)")
    if not pk_sql:
        print("[!] No se detectó PK en la tabla origen. Deberás asegurarte de tener una UNIQUE/PK equivalente en MySQL si usas UPSERT.")

    # Selección de columnas a actualizar
    upd_cols = choose_columns_interactive(safe_cols)

    # WHERE opcional en SQL Server (para filtrar filas a traer)
    where_clause = ask("WHERE opcional en SQL Server (sin 'WHERE', deja vacío para todas)", "")

    # Validaciones en MySQL (para UPSERT necesitamos PK)
    pk_mysql = fetch_mysql_pk_columns(mysql_cur, mysql_db, mysql_table)
    if mode.startswith("UPSERT"):
        if not pk_mysql:
            print("[!] La tabla destino en MySQL no tiene PRIMARY KEY. UPSERT requiere PK/UNIQUE. Cancela o crea una PK y vuelve a intentar.")
            sys.exit(1)
        if pk_sql and (set(map(str.lower, pk_sql)) - set(map(str.lower, pk_mysql))):
            print("[!] Aviso: las columnas PK en SQL Server no coinciden con la PK de MySQL. Aun así intentaremos, pero el UPSERT depende de la PK en MySQL.")

    # Confirmación
    print("\nResumen:")
    print(f"  Origen:    SQL Server {sql_db}.{schema}.{src_table}")
    print(f"  Destino:   MySQL {mysql_db}.{mysql_table}")
    print(f"  Modo:      {mode}")
    print(f"  PK SS:     {pk_sql or 'N/A'}")
    print(f"  PK MySQL:  {pk_mysql or 'N/A'}")
    print(f"  Columnas a actualizar: {', '.join(upd_cols)}")
    print(f"  Filtro WHERE: {where_clause or '(ninguno)'}")
    if not confirm("¿Continuar?", True):
        print("Cancelado por el usuario.")
        sys.exit(0)

    try:
        if mode.startswith("UPDATE"):
            if not pk_sql:
                print("[!] UPDATE requiere PK para armar el WHERE. Cancelo.")
                sys.exit(1)
            update_only(sql_cur, mysql_cnx, mysql_cur, schema, src_table, pk_sql, upd_cols,
                        batch_size=10_000, where_clause=where_clause or None)
        else:
            # UPSERT
            if not pk_mysql:
                print("[!] UPSERT requiere PK/UNIQUE en MySQL. Cancelo.")
                sys.exit(1)
            # Si no hay PK en SQL Server, igual se puede si sabemos qué columnas son PK en MySQL,
            # pero entonces necesitamos traer esas columnas desde SQL Server también.
            pk_for_upsert = pk_sql if pk_sql else pk_mysql
            # Asegura que las PK estén incluidas en la extracción:
            need_cols = list(dict.fromkeys(pk_for_upsert + upd_cols))
            upsert_mode(sql_cur, mysql_cnx, mysql_cur, schema, src_table,
                        pk_for_upsert, need_cols, batch_size=10_000, where_clause=where_clause or None)

        print("\n[🎉] Proceso finalizado.")
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print("\n[!] Error durante la actualización:", e)
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
