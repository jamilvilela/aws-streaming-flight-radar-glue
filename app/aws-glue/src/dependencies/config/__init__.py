"""
Config package — re-exports the configuration dataclasses.

This sub-package holds the JSON configuration files (``config.json``)
and re-exports the dataclasses defined in :mod:`..config_models` so
that consumers can import them from ``src.dependencies.config``.
"""

from ..config_models import (  # noqa: F401
    CdcConfig,
    Config,
    PartitionKey,
    SchemaField,
    SourceConfig,
    TargetConfig,
)
