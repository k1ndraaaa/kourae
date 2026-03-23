from adapters.Redis.MainClass import RedisClient
from typing import Dict, Optional
#así sea postgresql, mysql, mariadb, en un futuro presentarían el mismo frontend
from adapters.Postgresql.MainClass import PostgresClient as SqlClient, Select
from native.Library.commons import DatabaseSchema, Session
import json

import json
import hashlib
from typing import Dict, Optional


class Economizador:
    def __init__(
        self,
        redis_client: RedisClient,
        sql_client: SqlClient,
        session: Optional[Session] = None,
        ttl: Optional[int] = 1440
    ):
        self.redis_server = redis_client
        self.sql_server = sql_client
        self.known_schemas: Dict[str, DatabaseSchema] = {}
        self.session = session
        self.ttl = ttl
    def _stable_hash(self, value: str):
        return hashlib.sha256(value.encode()).hexdigest()
    def _get_session(self, session):
        return session or self.session
    def _build_cache_key(self, sql, params, session):
        raw = f"{sql}:{params}"
        query_hash = self._stable_hash(raw)
        addr = session.address if session else "public"
        return f"{addr}:{query_hash}"
    def _row_hash(self, row):
        raw = json.dumps(row, sort_keys=True)
        return self._stable_hash(raw)
    def register_schema(self, schema_name="public"):
        schema = self.sql_server.extract_schema(schema_name)
        if schema.id in self.known_schemas:
            raise ValueError(f"Schema '{schema.id}' ya registrado")
        self.known_schemas[schema.id] = schema
    def refresh_schema(self, schema_name="public"):
        schema = self.sql_server.extract_schema(schema_name)
        self.known_schemas[schema.id] = schema
    def select(self, built_query, session: Optional[Session] = None):
        s = self._get_session(session)
        sql, params = built_query.build()
        cache_key = self._build_cache_key(sql, params, s)
        cached = self.redis_server.get(cache_key)
        if cached:
            return json.loads(cached)
        result = self.sql_server.query(sql, params, fetch=True)
        columns_requested = built_query.get_columns()
        records = [
            dict(zip(columns_requested, row))
            for row in result
        ]
        table_name = (
            built_query.table.name
            if hasattr(built_query.table, "name")
            else built_query.table
        )
        for row in records:
            row_id = row.get("id")
            if row_id is None:
                continue
            row_hash = self._row_hash(row)
            self.redis_server.hset(
                f"table:{table_name}",
                row_id,
                row_hash
            )
            self.redis_server.sadd(
                f"row:{row_id}:{table_name}",
                cache_key
            )
        self.redis_server.set(
            cache_key,
            json.dumps(records),
            ex=self.ttl
        )
        return records
    def fetchone(self, built_query, session=None):
        rows = self.select(built_query, session)
        return rows[0] if rows else None
    def scalar(self, built_query, session=None):
        row = self.fetchone(built_query, session)
        if not row:
            return None
        return next(iter(row.values()))
    def update(self, built_query, session=None):
        sql, params = built_query.build()
        self.sql_server.query(sql, params)
        table_obj = built_query.table
        table_name = (
            table_obj.name
            if hasattr(table_obj, "name")
            else table_obj
        )
        columns = (
            list(table_obj.columns.keys())
            if hasattr(table_obj, "columns")
            else None
        )
        if not built_query._where:
            return
        select_query = Select(table_obj).columns("*")
        for cond in built_query._where:
            select_query.where(cond)
        affected_rows = self.sql_server.query(
            *select_query.build(),
            fetch=True
        )
        if not columns:
            columns = [
                col[0]
                for col in self.sql_server.query(
                    "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
                    (table_name,),
                    fetch=True
                )
            ]
        records = [
            dict(zip(columns, row))
            for row in affected_rows
        ]
        for row in records:
            row_id = row.get("id")
            if row_id is None:
                continue
            new_hash = self._row_hash(row)
            old_hash = self.redis_server.hget(
                f"table:{table_name}",
                row_id
            )
            if new_hash != old_hash:
                self.redis_server.hset(
                    f"table:{table_name}",
                    row_id,
                    new_hash
                )
                cache_keys = self.redis_server.smembers(
                    f"row:{row_id}:{table_name}"
                ) or []
                for cache_key in cache_keys:
                    self.redis_server.delete(cache_key)
    def delete(self, built_query, session=None):
        table_obj = built_query.table
        table_name = (
            table_obj.name
            if hasattr(table_obj, "name")
            else table_obj
        )
        columns = (
            list(table_obj.columns.keys())
            if hasattr(table_obj, "columns")
            else None
        )
        if built_query._where:
            select_query = Select(table_obj).columns("*")
            for cond in built_query._where:
                select_query.where(cond)
            affected_rows = self.sql_server.query(
                *select_query.build(),
                fetch=True
            )
            if not columns:
                columns = [
                    col[0]
                    for col in self.sql_server.query(
                        "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
                        (table_name,),
                        fetch=True
                    )
                ]
            records = [
                dict(zip(columns, row))
                for row in affected_rows
            ]
            for row in records:
                row_id = row.get("id")
                if row_id is None:
                    continue
                self.redis_server.hdel(
                    f"table:{table_name}",
                    row_id
                )
                cache_keys = self.redis_server.smembers(
                    f"row:{row_id}:{table_name}"
                ) or []
                for cache_key in cache_keys:
                    self.redis_server.delete(cache_key)
                self.redis_server.delete(
                    f"row:{row_id}:{table_name}"
                )
        sql, params = built_query.build()
        self.sql_server.query(sql, params)