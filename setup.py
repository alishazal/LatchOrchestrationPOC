from setuptools import setup, find_packages

setup(
    name="latch-orchestration-poc",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "networkx>=2.5",
        "graphviz>=0.16",
    ],
    python_requires=">=3.7",
)
