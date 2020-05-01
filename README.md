# KafkaCon

Create a new Consumer instance using the provided configuration, poll value from the offset and decrypt field(s) using AWS CMK (optional).

## Prerequisites
1. Use `python3.6`

2. Install pip and dev tools
```bash
$ apt install python3-pip python3-dev
```
3. Install requirements
```bash
$ pip install -r requirements.txt
```

## Usage
```
usage: kafkacon.py
       [-h] [--brokers [BROKERS [BROKERS ...]]] [--offset OFFSET]
       [--topic TOPIC] [--groupid GROUPID] [--filename FILENAME]
       [--debug DEBUG]
```
All required optional arguments can override configuration file settings.

Optional arguments:
```
  -h, --help            show this help message and exit
  --brokers [BROKERS [BROKERS ...]]
                        The List of brokers to connect (required)
  --offset OFFSET       The offset to seek to (required)
  --topic TOPIC         The topic (required)
  --groupid GROUPID     Client group id string. All clients sharing the same
                        group.id belong to the same group (required)
  --filename FILENAME   The filename to read configuration for KMS/Kafka
                        (optional, default: config.yaml)
  --debug DEBUG         Debug level (0..3) (optional)
```
## Example
```bash
$ AWS_PROFILE=myprofile python kafkacon.py --brokers localhost:9092 localhost:9093 localhost:9094 --offset 111 --topic mytopic --groupid mygroup
```

## Confguration example

See example.yaml
```
Kafka:
  brokers: 'brook.nonprod.us-west-2.aws.com:9092'
  offset: 213
  topic: mytopic
  groupid: mygroup
  schema.registry: 'https://mygroup.xxx:xxx@schema-registry.nonprod.us-west-2.aws.com'
  properties:
    sasl.mechanisms: SCRAM-SHA-512
    sasl.username: mygroup.xxx
    sasl.password: xxx
    security.protocol: SASL_SSL

KMS:
  KeyId: alias/main/default
  region: us-west-2
  EncryptionAlgorithm: 'SYMMETRIC_DEFAULT'
```
### Kafka main options:
* `brokers` - The List of brokers to connect (required)
* `offset` - The offset to seek to (required)
* `topic` - The topic (required)
* `groupid` - Client group id string. All clients sharing the same group.id belong to the same group. (required)

### Kafka Client properties:
* `sasl.mechanisms` - SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER. NOTE: Despite the name only one mechanism must be configured.
* `sasl.username` - SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
* `sasl.password` - SASL password
* `security.protocol` - Protocol used to communicate with brokers. Supported: `SASL_SSL`

Full list of configuration options is here: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

## SchemaRegistry options:
* `schema.registry` - SchemaRegistry url

### KMS options:
* `region (string)` -- AWS region

* `KeyId (string)` --
Specifies the customer master key (CMK) that AWS KMS will use to decrypt the ciphertext. Enter a key ID of the CMK that was used to encrypt the ciphertext.

If you specify a KeyId value, the Decrypt operation succeeds only if the specified CMK was used to encrypt the ciphertext.

This parameter is required only when the ciphertext was encrypted under an asymmetric CMK. Otherwise, AWS KMS uses the metadata that it adds to the ciphertext blob to determine which CMK was used to encrypt the ciphertext. However, you can use this parameter to ensure that a particular CMK (of any kind) is used to decrypt the ciphertext.

To specify a CMK, use its key ID, Amazon Resource Name (ARN), alias name, or alias ARN. When using an alias name, prefix it with "alias/" .

For example:
```
Key ID: 1234abcd-12ab-34cd-56ef-1234567890ab
Key ARN: arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab
Alias name: alias/ExampleAlias
Alias ARN: arn:aws:kms:us-east-2:111122223333:alias/ExampleAlias
```
To get the key ID and key ARN for a CMK, use ListKeys or DescribeKey . To get the alias name and alias ARN, use ListAliases .

* `EncryptionAlgorithm (string)` --
Specifies the encryption algorithm that will be used to decrypt the ciphertext. Specify the same algorithm that was used to encrypt the data. If you specify a different algorithm, the Decrypt operation fails.

This parameter is required only when the ciphertext was encrypted under an asymmetric CMK. The default value, `SYMMETRIC_DEFAULT` , represents the only supported algorithm that is valid for symmetric CMKs.

Supported: `'SYMMETRIC_DEFAULT'|'RSAES_OAEP_SHA_1'|'RSAES_OAEP_SHA_256'`

## Links
* https://docs.confluent.io/current/clients/confluent-kafka-python/#pythonclient-configuration
* https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
* https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kms.html#KMS.Client.decrypt
