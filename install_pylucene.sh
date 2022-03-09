# Because pylucene is a pain to install,
# this script will try to install it for you.

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

# Install jcc.
pipenv shell
cd pylucene
pushd jcc
python setup.py build
popd

# Install pylucene.
export PATH="$PATH:$(pwd)/ant/bin"
ANT=$(which ant) PYTHON=$(which python) JCC="python -m jcc --shared --arch x86_64 --wheel" NUM_FILES=10 make

# Clean up the files.
cd ../
rm -rf ant
