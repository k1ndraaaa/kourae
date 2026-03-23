from adapters.EnvLoader.MainClass import *
from native.Library.commons import SqlClient, TableSchema, DatabaseSchema, Column, build_conditions, build_set, Condition
from adapters.Postgresql.Errors import *
import psycopg2
from psycopg2 import pool
from typing import Dict
from contextlib import contextmanager

class Table:
    def __init__(
        self,
        sql_client: SqlClient,
        table_name: str,
        columns: Dict[str, str] | None = None,
        auto_schema: bool = False
    ):
        if not columns and not auto_schema:
            raise ValueError(
                "Either columns or auto_schema must be provided"
            )
        self.client = sql_client
        self.name = table_name
        self.columns = {}
        if columns:
            self._load_columns(columns)
        elif auto_schema:
            schema = self.client.extract_schema()
            table_schema = schema.tables.get(table_name)
            if not table_schema:
                raise ValueError(f"Table '{table_name}' not found in schema")
            self._load_columns(table_schema.columns)
    def _load_columns(self, columns: Dict[str, str]):
        for name, dtype in columns.items():
            col = Column(self, name, dtype)
            self.columns[name] = col
            setattr(self, name, col)
    def select(self):
        return Select(self.name)
    def insert(self):
        return Insert(self.name)
    def update(self):
        return Update(self.name)
    def delete(self):
        return Delete(self.name)

class Query:
    def __init__(self, table):
        if isinstance(table, Table):
            table = table.name
        self.table = table
        self._params = []
    def params(self):
        return tuple(self._params)
    def _reset_params(self):
        self._params = []
    def build(self):
        raise NotImplementedError
    
class Select(Query):
    def __init__(self, table):
        super().__init__(table)
        self._columns = ["*"]
        self._joins = []
        self._where = []
        self._group = []
        self._having = []
        self._order = []
        self._limit = None
        self._offset = None
    def columns(self, *cols):
        self._columns = cols
        return self
    def join(self, table, conditions, kind="INNER"):
        """
        conditions: Condition | list[Condition]
        """
        if isinstance(table, Table):
            table = table.name
        if not isinstance(conditions, list):
            conditions = [conditions]
        self._joins.append((kind, table, conditions))
        return self
    def where(self, condition):
        if isinstance(condition, list):
            self._where.extend(condition)
        else:
            self._where.append(condition)
        return self
    def group_by(self, *cols):
        self._group.extend(cols)
        return self
    def having(self, column, operator, value):
        self._having.append((column, operator, value))
        return self
    def order_by(self, *cols):
        self._order.extend(cols)
        return self
    def limit(self, n):
        self._limit = n
        return self
    def offset(self, n):
        self._offset = n
        return self
    def build(self):
        self._reset_params()
        sql = f"SELECT {', '.join(self._columns)} FROM {self.table}"
        for kind, table, conds in self._joins:
            join_sql_parts = []
            for cond in conds:
                col = cond.column
                op = cond.operator
                val = cond.value
                if isinstance(val, Column):
                    join_sql_parts.append(f"{col} {op} {val}")
                else:
                    join_sql_parts.append(f"{col} {op} %s")
                    self._params.append(val)
            join_sql = " AND ".join(join_sql_parts)
            sql += f" {kind} JOIN {table} ON {join_sql}"
        if self._where:
            where_sql, params = build_conditions(self._where)
            sql += f" WHERE {where_sql}"
            self._params.extend(params)
        if self._group:
            sql += " GROUP BY " + ", ".join(self._group)
        if self._having:
            having_sql, params = build_conditions(self._having)
            sql += f" HAVING {having_sql}"
            self._params.extend(params)
        if self._order:
            sql += " ORDER BY " + ", ".join(self._order)
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"
        return sql, self.params()
    def get_columns(self):
        return self._columns
class Insert(Query):
    def __init__(self, table):
        super().__init__(table)
        self._data = {}
        self._returning = None

    def values(self, **kwargs):
        self._data.update(kwargs)
        return self

    def returning(self, *cols):
        self._returning = cols
        return self

    def build(self):
        self._reset_params()

        columns = ", ".join(self._data.keys())
        placeholders = ", ".join(["%s"] * len(self._data))

        self._params.extend(self._data.values())

        sql = f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders})"

        if self._returning:
            sql += " RETURNING " + ", ".join(self._returning)

        return sql, self.params()
class Update(Query):
    def __init__(self, table):
        super().__init__(table)
        self._data = {}
        self._where = []
    def set(self, **kwargs):
        self._data.update(kwargs)
        return self
    def where(self, condition):
        if isinstance(condition, list):
            self._where.extend(condition)
        else:
            self._where.append(condition)
        return self
    def build(self):
        self._reset_params()
        set_sql, set_params = build_set(self._data)
        sql = f"UPDATE {self.table} SET {set_sql}"
        self._params.extend(set_params)
        if self._where:
            where_sql, params = build_conditions(self._where)
            sql += f" WHERE {where_sql}"
            self._params.extend(params)
        return sql, self.params()
class Delete(Query):
    def __init__(self, table):
        super().__init__(table)
        self._where = []
    def where(self, condition):
        if isinstance(condition, list):
            self._where.extend(condition)
        else:
            self._where.append(condition)
        return self
    def build(self):
        self._reset_params()
        sql = f"DELETE FROM {self.table}"
        if self._where:
            where_sql, params = build_conditions(self._where)
            sql += f" WHERE {where_sql}"
            self._params.extend(params)
        return sql, self.params()

class PostgresClient(SqlClient):
    def __init__(
        self, 
        host:str, 
        port:int,
        user:str,
        password:str,
        database: str,
        min_connections: int = 1,
        max_connections: int = 3
    ):
        try:
            self.client = psycopg2.pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                dbname=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except Exception as e:
            raise AdapterError(e) from e
    def healthcheck(self) -> bool:
        try:
            with self.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            return True
        except Exception as e:
            raise AdapterError(f"PostgreSQL no responde: {e}") from e
    @contextmanager
    def get_conn(self):
        conn = self.client.getconn()
        try:
            yield conn
        finally:
            self.client.putconn(conn)
    def query(self, sql_or_query, params=None, fetch=False):
        if hasattr(sql_or_query, "build"):
            sql, params = sql_or_query.build()
        else:
            sql = sql_or_query
            params = params or ()
        with self.get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    if fetch:
                        result = cur.fetchall()
                    else:
                        result = None
                    if not fetch:
                        conn.commit()
                    return result
            except Exception as e:
                conn.rollback()
                raise PostgresClientError(e) from e
    def scalar(self, query):
        result = self.query(query, fetch=True)
        return result[0][0] if result else None
    def fetchone(self, query):
        if hasattr(query, "build"):
            sql, params = query.build()
        else:
            sql, params = query, ()
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
    def exists(self, query):
        q = query.columns("1").limit(1)
        return bool(self.scalar(q))
    def extract_schema(self, schema_name="public") -> DatabaseSchema:
        tables = self.query(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
            """,
            (schema_name,),
            fetch=True
        )
        schema = DatabaseSchema(id=schema_name)
        for (table_name,) in tables:
            columns = self.query(
                """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                """,
                (schema_name, table_name),
                fetch=True
            )
            schema.tables[table_name] = TableSchema(
                name=table_name,
                columns={col: dtype for col, dtype in columns}
            )
        schema.compute_version()
        return schema
    def create_table(self, table: Table, if_not_exists=True):
        parts = []
        for col_name, col in table.columns.items():
            parts.append(f"{col_name} {col.dtype}")
        sql = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{table.name} ({', '.join(parts)})"
        with self.get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    conn.commit()
            except Exception as e:
                conn.rollback()
                raise PostgresClientError(e) from e