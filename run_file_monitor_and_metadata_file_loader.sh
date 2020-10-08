# execute file monitoring applicaton by calling its command file
/ext_data/shared/.apps/monitor_files/monitor_files_v1.0/run_file_monitoring.sh
# execute metadata file loader
cd /ext_data/shared/.apps/metadata_file_loader/metadata_file_loader_v2.1
source .venv/bin/activate
python metadata_file_loader.py
deactivate