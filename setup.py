from setuptools import setup, find_packages

setup(
    name="etl-pipeline-project",
    version="1.0.0",
    description="End-to-End ETL Pipeline with Automated Weather Reporting",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.28.0",
        "pandas>=1.5.0",
        "PyYAML>=6.0",
        "Jinja2>=3.1.0",
    ],
    extras_require={
        "dev": ["pytest>=7.2.0"],
    },
)
