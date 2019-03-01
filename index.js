const AWS = require("aws-sdk");
const Transform = require('stream').Transform;
const async = require('async');
const uuidv4 = require('uuid/v4');

class SQSError {
  constructor(message, code) {
    this.message = message;
    this.code = code
  }
  toString() {
    return `Error ${this.code}: ${this.message}`;
  }
}

class SQSClient {
  constructor(awsRegion) {
    this.region = awsRegion;
    AWS.config.update({region:awsRegion});
    this.sqs = new AWS.SQS();
  }

  sendMessage(message, attributes, messageGroupId, queueUrl) {
    return new Promise( (resolve, reject) => {
      const msg = {
        MessageBody: message, /* required */
        QueueUrl: queueUrl,
        DelaySeconds: 0,
        MessageAttributes: attributes,
        MessageDeduplicationId: uuidv4(),
        MessageGroupId: messageGroupId
      }

      this.sqs.sendMessage(msg, (err, data) => {
        if (err) {
          console.error(err);
          return reject(err);
        }
        return resolve();
      });
    });
  }

  getQueueAttributes(queueUrl) {
    return new Promise( (resolve, reject) => {
      const params = {
        QueueUrl: queueUrl, /* required */
        AttributeNames: [
          'All',
        ]
      };

      this.sqs.getQueueAttributes(params, (err, data) => {
        if (err) {
          console.log(err);
          return reject(err);
        }

        return resolve(data.Attributes);
      } );
    });
  }

  createIfNotExists(queueName, attributes) { //Idempotent function only creates a topic if not exists
    return new Promise( (resolve, reject) => {
      const queryParams = {
        QueueName: queueName
      }

      this.sqs.getQueueUrl(queryParams, (err, data) => {
        if (err) {
          if (err.code == 'AWS.SimpleQueueService.NonExistentQueue') {  // Queue not exists, creating
            const params = {
              QueueName: queueName,
              Attributes: attributes
            }

            this.sqs.createQueue(params, (err, data) => {
              if (err) {
                console.log(err);
                return reject(err);
              }
              return resolve(data.QueueUrl);
            })
          }
          else {
            console.log(err);
            return reject(err);
          }
        }
        return reject(new SQSError(`Queue exists: ${queueName}`, 'QUEUE_EXISTS'));
      })
    });
  }
}

module.exports = SQSClient;
