# --------------------------------------------
# Because pylucene is a pain to install,
# this script will try to install it for you.
# --------------------------------------------

# https://intoli.com/blog/exit-on-errors-in-bash-scripts/
set -e
# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

# Change these variables as needed.
mirror=downloads
lucene_version=9.4.1
ant_version=1.10.12

# Download pylucene and ant.
curl -O https://${mirror}.apache.org/lucene/pylucene/pylucene-${lucene_version}-src.tar.gz
gunzip pylucene-${lucene_version}-src.tar.gz
tar -xvf pylucene-${lucene_version}-src.tar
mv pylucene-${lucene_version} pylucene
rm pylucene-${lucene_version}-src.tar

curl -O https://${mirror}.apache.org/ant/binaries/apache-ant-${ant_version}-bin.tar.gz
gunzip apache-ant-${ant_version}-bin.tar.gz
tar -xvf apache-ant-${ant_version}-bin.tar
rm apache-ant-${ant_version}-bin.tar
mv apache-ant-${ant_version} ant
export PATH="$PATH:$(pwd)/ant/bin"

# Install jcc.
# https://lucene.apache.org/pylucene/jcc/install.html
cd pylucene
pushd jcc
# Use the following line for M1/M2 Macs:
# export JCC_INCLUDES=/opt/homebrew/Cellar/openjdk/19.0.1/libexec/openjdk.jdk/Contents/Home/include:/opt/homebrew/Cellar/openjdk/19.0.1/libexec/openjdk.jdk/Contents/Home/include/darwin:/opt/homebrew/opt/python@3.10/Frameworks/Python.framework/Versions/3.10/include/python3.10
# Also for M1/M2 Macs:
# in setup.py, change: enable_shared = True
python setup.py build
python setup.py install
popd

# Install pylucene.
# https://lucene.apache.org/pylucene/install.html
# Might need `--arch x86_64`.
ANT=$(which ant) PYTHON=$(which python) JCC="python -m jcc --shared --wheel" NUM_FILES=10 make

# Clean up the files.
cd ../
pipenv install pylucene/dist/*.whl
pipenv update
true
