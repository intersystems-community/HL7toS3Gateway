# HL7toS3Gateway

HL7 to S3 gateway is a lihtweight process, allowing to capture HL7v2 messages sent 
over TCP/MLLP protocol and upload them to the S3 bucket. These messages can be then processed by
HealthShare Message Transformation Service.

HL7toS3Gateway envisioned to be deployed within secure network of cloud VPC of the organization, so unencrypted HL7 TCP traffic never exposed to the public internet.

## Build
```
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
```

## Run
```
export AWS_ACCESS_KEY_ID=AKIA4ESCYD67GWODXXXX
export AWS_SECRET_ACCESS_KEY='Ac/JNZd2uslCgLcEsy9HAVYkOQLW1Vsi2veXXXXX'
export S3_BUCKET=anton-demo-bucket

java -cp target/hl7tos3-1.0-SNAPSHOT-jar-with-dependencies.jar com.antonum.hl7tos3.MLLPBasedHL7ThreadedServer
```

## Test
```
echo -n -e "\x0b$(cat hl7-file.txt )\x1c\x0d" | netcat localhost 1080
```

GUI tool like HL7 Inspector - send messages to port 1080
## References

HL7 listener implementation is based on:

https://saravanansubramanian.com/hl72xjavaprogramming/

https://github.com/SaravananSubramanian/hl7 

S3 client - AWS Java SDK