"""
infra/sales_db_hook.py
Hook custom para conectarse al DW de ventas (target Postgres / RDS).

Extiende PostgresHook de Airflow, que ya maneja:
- La conexión via Airflow Connections (UI o env vars)
- El pool de conexiones
- El manejo de cursores y commits

Uso desde una task:
    from infra.sales_db_hook import SalesDbHook

    hook = SalesDbHook()
    hook.run("INSERT INTO sales ...")
    records = hook.get_records("SELECT * FROM sales WHERE sale_date = %s", [fecha])
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook


class SalesDbHook(PostgresHook):
    """
    Hook para el DW de ventas.
    Usa la Airflow Connection con conn_id='sales_db' que configurás
    en la UI de Airflow o via variable de entorno:
        AIRFLOW_CONN_SALES_DB=postgresql://sales_user:sales_pass@target-postgres:5432/sales
    """

    conn_name_attr = "sales_db_conn_id"
    default_conn_name = "sales_db"

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__(postgres_conn_id=conn_id)

    def insert_rows_idempotent(self, rows: list[dict]) -> int:
        """
        Inserta filas en la tabla sales usando ON CONFLICT DO NOTHING.
        Retorna la cantidad de filas efectivamente insertadas.
        """
        if not rows:
            return 0

        sql = """
            INSERT INTO sales (order_id, customer, product, quantity, unit_price, region, sale_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
        """

        conn = self.get_conn()
        inserted = 0

        try:
            with conn:
                with conn.cursor() as cur:
                    for row in rows:
                        cur.execute(sql, (
                            row["order_id"],
                            row["customer"],
                            row["product"],
                            int(row["quantity"]),
                            float(row["unit_price"]),
                            row["region"],
                            row["sale_date"],
                        ))
                        if cur.rowcount > 0:
                            inserted += 1
        finally:
            conn.close()

        return inserted

    def run_quality_check(self, query: str, params: tuple) -> int:
        """
        Corre un query de quality check y retorna el count resultante.
        """
        records = self.get_records(query, parameters=params)
        return records[0][0] if records else 0