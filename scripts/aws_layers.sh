mkdir -p temp/python && cd temp/python
pip3 install requests  -t .
pip3 install --pre gql[all] -t .
cd ..
zip -r9 ../layer.zip .
aws lambda publish-layer-version --layer-name basic-layers --description "Layer containing requests and gpl" --zip-file fileb://../layer.zip --compatible-runtimes python3.6 python3.7 python3.8