from pathlib import Path

import yaml


class Config:
    """
    Loads configuration values from config/config.yaml.

    Example:
        Config.get("kafka.topic")
        Config.get("smart_meter.number_of_households")
    """

    _config = None

    _project_root = Path(__file__).resolve().parents[1]
    _config_path = _project_root / "config" / "config.yaml"

    @classmethod
    def load(cls):
        """
        Load the YAML configuration once.
        """

        if cls._config is None:

            if not cls._config_path.exists():
                raise FileNotFoundError(
                    f"Configuration file not found: "
                    f"{cls._config_path}"
                )

            with cls._config_path.open(
                "r",
                encoding="utf-8",
            ) as file:

                cls._config = yaml.safe_load(file)

        return cls._config

    @classmethod
    def get(cls, key, default=None):
        """
        Get a configuration value using dot notation.

        Example:
            Config.get("kafka.topic")
            Config.get("smart_meter.base_load.min_kwh")
        """

        config = cls.load()

        value = config

        for part in key.split("."):

            if not isinstance(value, dict):
                return default

            if part not in value:
                return default

            value = value[part]

        return value

    @classmethod
    def get_section(cls, section):
        """
        Return a complete configuration section.

        Example:
            Config.get_section("kafka")
        """

        return cls.get(section, {})