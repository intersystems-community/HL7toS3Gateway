# HL7toS3Gateway

## Build

mvn assembly:assembly -DdescriptorId=jar-with-dependencies

## Run

export AWS_ACCESS_KEY_ID=AKIA4ESCYD67GWODXXXX
export AWS_SECRET_ACCESS_KEY='Ac/JNZd2uslCgLcEsy9HAVYkOQLW1Vsi2veXXXXX'
export S3_BUCKET=anton-demo-bucket

java -cp target/hl7tos3-1.0-SNAPSHOT-jar-with-dependencies.jar com.antonum.hl7tos3.MLLPBasedHL7ThreadedServer

## Test

echo -n -e "\x0b$(cat hl7-file.txt )\x1c\x0d" | netcat localhost 1080

GUI tool like HL7 Inspector - send messages to port 1080
## References

HL7 listener implementation is based on:

https://saravanansubramanian.com/hl72xjavaprogramming/

https://github.com/SaravananSubramanian/hl7 

S3 client - AWS Java SDK