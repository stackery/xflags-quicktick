const AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event, context) => {
  const currentTime = new Date();

  // Log the event argument for debugging and for use in local development.
  console.log(JSON.stringify(event, undefined, 2));

  const rideId = event.pathParameters.rideId;
  const ticketId = event.pathParameters.ticketId;

  const { Item: ticket } = await ddb.get({
    TableName: process.env.TICKETS_TABLE_NAME,
    Key: {
      ticketId
    }
  }).promise();

  console.log(ticket);

  if (!ticket) {
    return {
      statusCode: 404
    };
  }

  if (ticket.rideId !== rideId) {
    return {
      statusCode: 404
    };
  }

  const minReturnTime = new Date(ticket.returnTime);
  const maxReturnTime = new Date(minReturnTime.getTime() + 60*1000);

  if (currentTime.getTime() < minReturnTime.getTime()) {
    return {
      statusCode: 400,
      body: `Ticket is not valid until ${minReturnTime}`
    };
  }

  if (currentTime.getTime() > maxReturnTime.getTime()) {
    return {
      statusCode: 400,
      body: `Nicki says NO!`
    };
  }

  return {
    statusCode: 200,
    body: 'AM says YES!!!'
  }

  return {};
};
