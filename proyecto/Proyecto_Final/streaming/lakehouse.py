"""Canonical Delta Lake locations for the medallion architecture."""
from dataclasses import dataclass


FACT_TABLE = "fact_desempeno_envio"
KPI_TABLE = "indicadores_cumplimiento_diario"
DIMENSION_TABLES = (
    "dim_fecha",
    "dim_ruta",
    "dim_ciudad",
    "dim_centro_logistico",
    "dim_vehiculo",
    "dim_estado_envio",
)


@dataclass(frozen=True)
class LakehousePaths:
    """Build storage paths without assuming a local filesystem implementation."""

    root: str

    def _join(self, *parts: str) -> str:
        return "/".join((self.root.rstrip("/"), *parts))

    @property
    def bronze_events(self) -> str:
        return self._join("bronze", "events")

    @property
    def silver_events(self) -> str:
        return self._join("silver", "events")

    @property
    def silver_rejected_events(self) -> str:
        return self._join("silver", "rejected_events")

    @property
    def silver_current_shipments(self) -> str:
        return self._join("silver", "current_shipments")

    @property
    def gold_root(self) -> str:
        return self._join("gold")

    def gold_table(self, table_name: str) -> str:
        if not table_name or "/" in table_name or "\\" in table_name:
            raise ValueError("Gold table name must be a non-empty path segment")
        return self._join("gold", table_name)

    @property
    def checkpoint(self) -> str:
        return self._join("checkpoints", "main")
