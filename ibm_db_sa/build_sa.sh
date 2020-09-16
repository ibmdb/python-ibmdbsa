CURR_PATH=`pwd`
SITE_PKG_PATH="/Library/Frameworks/Python.framework/Versions/3.8/lib/python3.8/site-packages"
rm -rf build
cd $SITE_PKG_PATH
rm -rf ibm_db_sa-0.3.5-py3.8.egg
rm -rf ibm_db-3.0.2-py3.8-macosx-10.9-x86_64.egg
cd $CURR_PATH
python3 setup.py install
