"""
Config package — re-exports dataclasses from the unified config_models module.

This package exists mainly to hold the JSON configuration files
(config.json). All dataclasses have been moved to
:mod:`..config_models` so they live alongside other .py modules.
"""

from ..config_models import (  # noqa: F401
    CdcConfig,
    Config,
    PartitionKey,
    SchemaField,
    SourceConfig,
    TargetConfig,
)

