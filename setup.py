from setuptools import setup, find_packages

setup(
    name="dremiometrics",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.2.0",
        "pyyaml>=5.4.0",
        "psutil>=5.8.0",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "dremiometrics=src.dremio.run_benchmarks:main",
        ],
    },
) 