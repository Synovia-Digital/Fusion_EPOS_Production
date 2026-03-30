"""
Synovia Fusion - Configuration Utilities (Enterprise Standard)

Design principles:
- No secrets in logs
- Deterministic resolution order: ENV -> INI -> DEFAULT/FAIL
- One place to handle config hygiene for all pipeline scripts
"""

from __future__ import annotations

import os
import configparser
from dataclasses import dataclass
from typing import Optional, Dict, Iterable


@dataclass(frozen=True)
class IniEnvContext:
    """
    Configuration context for scripts.

    Defaults are aligned with Fusion_EPOS production conventions but are overridable.
    """
    ini_path: str
    ini_section: str

    def _parser(self) -> configparser.ConfigParser:
        cfg = configparser.ConfigParser()
        if os.path.exists(self.ini_path):
            cfg.read(self.ini_path)
        return cfg

    def get_env(self, key: str, default: Optional[str] = None) -> Optional[str]:
        v = os.environ.get(key)
        if v is None:
            return default
        v = str(v).strip()
        return v if v != "" else default

    def get_ini(self, key: str, default: Optional[str] = None) -> Optional[str]:
        cfg = self._parser()
        if self.ini_section not in cfg:
            return default
        sec = cfg[self.ini_section]
        if key not in sec:
            return default
        v = str(sec.get(key, "")).strip()
        return v if v != "" else default

    def get_secret(self, *, env_key: str, ini_key: str, required: bool = True) -> str:
        """
        Resolve a secret (e.g., password) in locked order:
          ENV -> INI -> FAIL (if required)

        NOTE: This method returns the raw secret. Do not print/log it.
        """
        v = self.get_env(env_key)
        if v:
            return v

        v = self.get_ini(ini_key)
        if v:
            return v

        if required:
            raise RuntimeError(
                f"Required secret not found. Set env var '{env_key}' "
                f"or define '{ini_key}' in {self.ini_path} [{self.ini_section}]."
            )
        return ""

    def get_required_value(self, *, env_key: str, ini_key: str, label: str) -> str:
        v = self.get_env(env_key) or self.get_ini(ini_key)
        if not v:
            raise RuntimeError(
                f"Required configuration '{label}' not found. "
                f"Set env var '{env_key}' or define '{ini_key}' in {self.ini_path} [{self.ini_section}]."
            )
        return v

    def get_many(self, keys: Iterable[str]) -> Dict[str, str]:
        """
        Fetch multiple values from INI section if present (no env lookup).
        Useful for optional config blocks.
        """
        cfg = self._parser()
        out: Dict[str, str] = {}
        if self.ini_section not in cfg:
            return out
        sec = cfg[self.ini_section]
        for k in keys:
            if k in sec and str(sec[k]).strip():
                out[k] = str(sec[k]).strip()
        return out
