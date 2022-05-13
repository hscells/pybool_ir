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
lucene_version=8.11.0
ant_version=1.10.12

# Download pylucene and ant.
wget https://${mirror}.apache.org/lucene/pylucene/pylucene-${lucene_version}-src.tar.gz
gunzip pylucene-${lucene_version}-src.tar.gz
tar -xvf pylucene-${lucene_version}-src.tar
mv pylucene-${lucene_version} pylucene
rm pylucene-${lucene_version}-src.tar

wget https://${mirror}.apache.org/ant/binaries/apache-ant-${ant_version}-bin.zip
unzip apache-ant-${ant_version}-bin.zip
rm apache-ant-${ant_version}-bin.zip
mv apache-ant-${ant_version} ant
export PATH="$PATH:$(pwd)/ant/bin"

# Install jcc.
# https://lucene.apache.org/pylucene/jcc/install.html
cd pylucene
pushd jcc
python setup.py build
python setup.py install
popd

# Install pylucene.
# https://lucene.apache.org/pylucene/install.html
ANT=$(which ant) PYTHON=$(which python) JCC="python -m jcc --shared --arch x86_64 --wheel" NUM_FILES=10 make

# Clean up the files.
cd ../
pipenv lock
pipenv install pylucene/dist/*.whl
pipenv sync
true