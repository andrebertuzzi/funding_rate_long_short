cd ../src
zip -r ../package.zip utils *.py
cd ..
aws lambda update-function-code --function-name crypto-consumer --zip-file fileb://package.zip  --publish
