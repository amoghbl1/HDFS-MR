# amoghbl1
# Uses Makefile, just making this cause of the submission format

make client

cd Client && java -cp .:bin/:../protobuf-java-2.6.1.jar Client $1 $2 $3 
