# #!/bin/sh

transformer=schema/tql_2_json.py
tql=schema/orp-pbeta-gdb-schema.tql
json=schema/orp-gdb-schema.json
datapath=data/processed/
echo "Transforming schema TQL -> JSON"
python $transformer $tql $json
echo "Extracting graph elems from PARQUETs"
python data_preprocess.py 
echo "Building graph snapshot..."
python migrator_orp.py  --batch_size 100 --force 