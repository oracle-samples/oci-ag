# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os
import subprocess
import tomllib
from pathlib import Path
from typing import Optional

PYPROJECT_FILE_NAME = "pyproject.toml"


def get_project_root(start: Optional[Path] = None) -> Path:
    current = (start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent

    for path in [current, *current.parents]:
        if (path / PYPROJECT_FILE_NAME).exists():
            return path

    package_root = Path(__file__).resolve()
    for path in package_root.parents:
        if (path / PYPROJECT_FILE_NAME).exists():
            return path

    raise FileNotFoundError(f"Unable to find {PYPROJECT_FILE_NAME}")


def get_package_version(project_root: Optional[Path] = None) -> str:
    root = project_root or get_project_root()
    with (root / PYPROJECT_FILE_NAME).open("rb") as pyproject_file:
        pyproject = tomllib.load(pyproject_file)

    return str(pyproject["project"]["version"])


def get_git_commit(project_root: Optional[Path] = None, short: bool = True) -> Optional[str]:
    root = project_root or get_project_root()
    command = ["git", "-C", str(root), "rev-parse"]
    if short:
        command.append("--short=7")
    command.append("HEAD")

    try:
        env = os.environ.copy()
        env.pop("GIT_DIR", None)
        return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL, env=env).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def resolve_image_version(project_root: Optional[Path] = None) -> str:
    explicit_version = os.environ.get("DFA_IMAGE_VERSION")
    if explicit_version:
        return explicit_version

    root = project_root or get_project_root()
    package_version = get_package_version(root)

    commit = get_git_commit(root)
    if commit:
        return f"{package_version}-{commit}"

    return package_version
