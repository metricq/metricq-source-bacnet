from setuptools import find_packages, setup

setup(
    name="metricq_source_bacnet",
    version="0.1",
    author="TU Dresden",
    python_requires=">=3.6",
    packages=find_packages(),
    scripts=[],
    entry_points="""
      [console_scripts]
      metricq-source-bacnet=metricq_source_bacnet:source_cmd
      """,
    install_requires=[
        "aiomonitor",
        "click",
        "click-completion",
        "click_log",
        "metricq~=1.2",
        "bacpypes~=0.18.0",
    ],
    extras_require={"journallogger": ["systemd"]},
)
