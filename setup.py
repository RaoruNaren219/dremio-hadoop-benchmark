from setuptools import setup, find_packages

setup(
    name="dremio-hadoop-benchmark",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.2.0",
        "pyyaml>=5.4.0",
    ],
    python_requires=">=3.7",
) 