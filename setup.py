from setuptools import setup, find_packages
import os

# Read requirements from requirements/ folder
def read_requirements(filename):
    """Read requirements from a file in requirements/ folder."""
    req_file = os.path.join('requirements', filename)
    if os.path.exists(req_file):
        with open(req_file, 'r') as f:
            return [line.strip() for line in f
                    if line.strip() and not line.startswith('#')]
    return []

setup(
    packages=find_packages(),
    include_package_data=True,
    install_requires=read_requirements('requirements.txt'),
    extras_require={
        'dev': read_requirements('requirements-dev.txt'),
    },
)
