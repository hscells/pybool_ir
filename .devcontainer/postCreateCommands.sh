pip install uv
uv add /pybool_ir/pylucene/dist/*.whl
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JCC_JDK=${JAVA_HOME}
export JCC_ARGSEP=";"
export JCC_LFLAGS="-L$JAVA_HOME/lib;-ljava;-L$JAVA_HOME/lib/server;-ljvm;-Wl,-rpath=$JAVA_HOME/lib:$JAVA_HOME/lib/server"
export JCC="python -m jcc --wheel"
uv add /pybool_ir/pylucene/jcc
uv pip install -e .
uv sync