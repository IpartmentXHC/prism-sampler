from __future__ import annotations

import hashlib
import json
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 client hook hosts
    import tomli as tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = REPO_ROOT / "config"


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


def read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


@dataclass(frozen=True)
class SamplerConfig:
    values: dict[str, Any]
    source: Path

    def section(self, name: str) -> dict[str, Any]:
        return dict(self.values.get(name, {}))

    @property
    def target(self) -> dict[str, Any]:
        return self.section("target")

    @property
    def collector(self) -> dict[str, Any]:
        return self.section("collector")

    @property
    def sampling(self) -> dict[str, Any]:
        return self.section("sampling")

    def digest(self) -> str:
        payload = json.dumps(self.values, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


def load_config(path: Path) -> SamplerConfig:
    if not path.is_file():
        raise ValueError(f"configuration does not exist: {path}")
    local = read_toml(path)
    platform_name = local.get("sampling", {}).get("platform", "kunpeng-920")
    platform_path = CONFIG_ROOT / "platforms" / f"{platform_name}.toml"
    profiles_path = CONFIG_ROOT / "sampling" / "profiles.toml"
    if not platform_path.is_file():
        raise ValueError(f"unknown platform: {platform_name}")
    values = _merge(local, {"platform": read_toml(platform_path)})
    values = _merge(values, {"sampling_profiles": read_toml(profiles_path)["profiles"]})
    return SamplerConfig(values, path.resolve())


def validate_config(config: SamplerConfig) -> list[str]:
    missing = []
    for section, keys in {
        "target": ("host", "remote_root"),
        "collector": ("binary",),
        "yba": ("root",),
    }.items():
        values = config.section(section)
        missing.extend(f"{section}.{key}" for key in keys if not values.get(key))
    profile = config.sampling.get("profile", "policy")
    if profile not in config.values["sampling_profiles"]:
        missing.append(f"sampling.profile={profile} (unknown)")
    return missing
