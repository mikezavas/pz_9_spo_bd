import mysql.connector
import psycopg2
from psycopg2.extras import RealDictCursor
import csv


class SQLTable:
    def __init__(self, db_config, table_name, engine='mysql'):
        self.engine = engine.lower()
        self.db_config = db_config.copy()
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.columns = []

        self._setup_engine()
        self.connect()

        if self._check_table_exists():
            self._update_column_names()
        else:
            print(f"Таблица '{self.table_name}' не существует.")

    def _setup_engine(self):
        if self.engine == 'mysql':
            self.connector = mysql.connector
            if 'dbname' in self.db_config:
                self.db_config['database'] = self.db_config.pop('dbname')
        elif self.engine == 'postgresql':
            self.connector = psycopg2
            if 'database' in self.db_config:
                self.db_config['dbname'] = self.db_config.pop('database')
        else:
            raise ValueError("Поддерживаются только 'mysql' и 'postgresql'")

    def _get_cursor(self, dict_mode=False):
        if self.engine == 'mysql':
            return self.connection.cursor(dictionary=dict_mode)
        else:
            return self.connection.cursor(cursor_factory=RealDictCursor if dict_mode else None)

    def connect(self):
        try:
            self.connection = self.connector.connect(**self.db_config)
            self.cursor = self._get_cursor()
            print(f"Подключено к {self.engine.upper()}")
            return True
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return False

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            try:
                self.connection.close()
                print("Соединение закрыто")
            except Exception:
                pass

    def _check_table_exists(self):
        try:
            if self.engine == 'mysql':
                self.cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
            else:
                self.cursor.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = current_database() AND table_name = %s",
                    (self.table_name,)
                )
            return bool(self.cursor.fetchone())
        except:
            return False

    def _update_column_names(self):
        try:
            if self.engine == 'mysql':
                self.cursor.execute(f"DESCRIBE {self.table_name}")
                self.columns = [r[0] for r in self.cursor.fetchall()]
            else:
                self.cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (self.table_name,)
                )
                self.columns = [r[0] for r in self.cursor.fetchall()]
        except:
            self.columns = []

    def _build_where(self, filters=None, condition=None):
        params = []
        clauses = []
        if condition:
            clauses.append(condition)
        if filters and isinstance(filters, dict):
            for col, val in filters.items():
                if isinstance(val, (list, tuple)) and len(val) == 2:
                    op, v = val
                    clauses.append(f"{col} {op} %s")
                    params.append(v)
                else:
                    clauses.append(f"{col} = %s")
                    params.append(val)
        if clauses:
            return " WHERE " + " AND ".join(clauses), params
        return "", params

    # CRUD запросы с поддержкой фильтрации
    def insert(self, data):
        cur = self._get_cursor()
        try:
            cols = ', '.join(data.keys())
            vals = ', '.join(['%s'] * len(data))
            q = f"INSERT INTO {self.table_name} ({cols}) VALUES ({vals})"
            cur.execute(q, list(data.values()))
            self.connection.commit()
            print(f"INSERT: Добавлено в {self.table_name}")
            return True
        except Exception as e:
            print(f"Ошибка: {e}")
            self.connection.rollback()
            return False
        finally:
            cur.close()

    def update(self, data, filters=None, condition=None):
        cur = self._get_cursor()
        try:
            set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
            where, params = self._build_where(filters, condition)
            q = f"UPDATE {self.table_name} SET {set_clause}{where}"
            cur.execute(q, list(data.values()) + params)
            self.connection.commit()
            print(f"UPDATE: Обновлено в {self.table_name}")
            return True
        except Exception as e:
            print(f"Ошибка: {e}")
            self.connection.rollback()
            return False
        finally:
            cur.close()

    def delete(self, filters=None, condition=None):
        cur = self._get_cursor()
        try:
            where, params = self._build_where(filters, condition)
            q = f"DELETE FROM {self.table_name}{where}"
            cur.execute(q, params)
            self.connection.commit()
            print(f"DELETE: Удалено из {self.table_name}")
            return True
        except Exception as e:
            print(f"Ошибка: {e}")
            self.connection.rollback()
            return False
        finally:
            cur.close()

    def select(self, columns='*', filters=None, condition=None, order_by=None, limit=None):
        cur = self._get_cursor(dict_mode=True)
        try:
            where, params = self._build_where(filters, condition)
            q = f"SELECT {columns} FROM {self.table_name}{where}"
            if order_by:
                q += f" ORDER BY {order_by}"
            if limit:
                q += " LIMIT %s"
                params.append(limit)
            cur.execute(q, params)
            res = cur.fetchall()
            print(f"SELECT: Получено {len(res)} записей")
            return res
        except Exception as e:
            print(f"Ошибка: {e}")
            return []
        finally:
            cur.close()

    # сложные запросы джоин
    def join_query(self, other_table, on, join_type="INNER", columns="*", filters=None, condition=None):
        jt = join_type.upper()
        if jt not in ("INNER", "LEFT", "RIGHT", "FULL"):
            raise ValueError("Допустимые типы JOIN: INNER, LEFT, RIGHT, FULL")

        if jt == "FULL" and self.engine == "mysql":
            return self._full_join_mysql(other_table, on, columns, filters, condition)

        where, params = self._build_where(filters, condition)
        q = f"SELECT {columns} FROM {self.table_name} {jt} JOIN {other_table} ON {on}{where}"
        cur = self._get_cursor(dict_mode=True)
        try:
            cur.execute(q, params)
            return cur.fetchall()
        except Exception as e:
            print(f"Ошибка JOIN: {e}")
            return []
        finally:
            cur.close()

    def _full_join_mysql(self, other_table, on, columns, filters, condition):
        where, params = self._build_where(filters, condition)
        left = f"SELECT {columns} FROM {self.table_name} LEFT JOIN {other_table} ON {on}{where}"
        right = f"SELECT {columns} FROM {self.table_name} RIGHT JOIN {other_table} ON {on}{where}"
        cur = self._get_cursor(dict_mode=True)
        try:
            cur.execute(left, params)
            res_left = cur.fetchall()
            cur.execute(right, params)
            res_right = cur.fetchall()
            seen, merged = set(), []
            for r in res_left + res_right:
                key = tuple(sorted(r.items()))
                if key not in seen:
                    seen.add(key)
                    merged.append(r)
            return merged
        except Exception as e:
            print(f"Ошибка FULL JOIN (MySQL): {e}")
            return []
        finally:
            cur.close()

    # сложные запросы юнион
    def union_query(self, queries, distinct=True):
        """
        :param queries: список кортежей (sql_string, params_list)
        :param distinct: true -> юнион, false -> юнион all
        """
        op = "UNION DISTINCT" if distinct else "UNION ALL"
        full = f" {op} ".join([q for q, _ in queries])
        all_params = []
        for _, p in queries:
            if p:
                all_params.extend(p)
        cur = self._get_cursor(dict_mode=True)
        try:
            cur.execute(full, all_params)
            return cur.fetchall()
        except Exception as e:
            print(f"Ошибка UNION: {e}")
            return []
        finally:
            cur.close()

    # управление таблицами
    def create_table(self, columns_def):
        if self.engine == 'postgresql' and 'AUTO_INCREMENT' in columns_def.upper():
            columns_def = columns_def.replace('AUTO_INCREMENT', 'SERIAL')
        cur = self._get_cursor()
        try:
            exists = self._check_table_exists()
            q = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_def})"
            cur.execute(q)
            self.connection.commit()

            if exists:
                print(f"Таблица '{self.table_name}' уже существует. Структура не изменена.")
            else:
                print(f"CREATE TABLE: Таблица {self.table_name} создана")
                self._update_column_names()
            return True
        except Exception as e:
            print(f"Ошибка: {e}")
            self.connection.rollback()
            return False
        finally:
            cur.close()

    def drop_table(self):
        cur = self._get_cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.connection.commit()
            print(f"DROP TABLE: Таблица {self.table_name} удалена")
            return True
        except Exception as e:
            print(f"Ошибка: {e}")
            return False
        finally:
            cur.close()

    def show_structure(self):
        cur = self._get_cursor()
        try:
            if self.engine == 'mysql':
                cur.execute(f"DESCRIBE {self.table_name}")
            else:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default"
                    "FROM information_schema.columns WHERE table_name = %s",
                    (self.table_name,)
                )
            res = cur.fetchall()
            print("Структура таблицы:")
            for r in res:
                print(r)
            return res
        except Exception as e:
            print(f"Ошибка: {e}")
            return []
        finally:
            cur.close()

    def add_column(self, col_def):
        cur = self._get_cursor()
        try:
            cur.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {col_def}")
            self.connection.commit()
            print("Столбец добавлен")
            self._update_column_names()
        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            cur.close()

    def drop_column(self, col_name):
        cur = self._get_cursor()
        try:
            cur.execute(f"ALTER TABLE {self.table_name} DROP COLUMN {col_name}")
            self.connection.commit()
            print("Столбец удалён")
            self._update_column_names()
        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            cur.close()

    # экспорт\импорт csv
    def export_csv(self, filename):
        data = self.select()
        if not data:
            print("Нет данных для экспорта")
            return
        try:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            print("Экспорт в CSV выполнен")
        except Exception as e:
            print(f"Ошибка: {e}")

    def import_csv(self, filename):
        try:
            with open(filename, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.insert(row)
            print("Импорт из CSV выполнен")
        except Exception as e:
            print(f"Ошибка: {e}")
