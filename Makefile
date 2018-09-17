PYTHON_VERSION ?= 3
PIP := pip${PYTHON_VERSION}
PYTHON := python${PYTHON_VERSION}
ARGS :=

install:
	${PIP} install --upgrade bson cairocffi GitPython graphviz lxml pymongo ${ARGS}
	${PYTHON} setup.py install ${ARGS}
