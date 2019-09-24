const AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient();

const TICKETS_PER_TIME = 5;
const TIME_DURATION_MIN = 1;

exports.handler = async (event, context) => {
  // Log the event argument for debugging and for use in local development.
  console.log(JSON.stringify(event, undefined, 2));

  const rideId = event.pathParameters.rideId;
  let ticketId;

  while (true) {
    const nextReturnTime = await getNextReturnTime(rideId);

    try {
      ticketId = await createTicket(rideId, nextReturnTime);
      break;
    } catch (err) {
      /* It's possible another ticket was issued and the next return time is no
       * longer valid by the time we create a ticket. Iterate through the loop
       * once more. */
      if (err.code !== 'ConditionalCheckFailedException') {
        throw err;
      }
    }
  }

  return {
    body: JSON.stringify({
      rideId,
      ticketId,
    }, null, 2)
  };
};

const getNextTimeSlot = () => {
  const currentTimeInMS = new Date().getTime();
  const durationOffset = currentTimeInMS % (TIME_DURATION_MIN * 60 * 1000);

  return new Date(currentTimeInMS - durationOffset + TIME_DURATION_MIN * 60 * 1000).toISOString();
};

const createRide = async rideId => {
  const nextReturnTime = getNextTimeSlot();

  await ddb.put({
    TableName: process.env.AVAILABLE_TICKETS_TABLE_NAME,
    Item: {
      rideId,
      nextReturnTime,
      availability: 5
    }
  }).promise();

  return nextReturnTime;
};

const addMinutesToISOString = (time, minutes) => {
  const currentTime = new Date(time);

  if (currentTime.getTime() < new Date().getTime()) {
    return getNextTimeSlot();
  } else {
    return new Date(currentTime.getTime() + minutes * 60 * 1000).toISOString();
  }
};

const getNextReturnTime = async rideId => {
  while (true) {
    const { Item: rideAvailability } = await ddb.get({
      TableName: process.env.AVAILABLE_TICKETS_TABLE_NAME,
      Key: {
        rideId
      }
    }).promise();

    // To make development easy, create a ride if it doesn't exist yet
    if (!rideAvailability) {
      return await createRide(rideId);
    }

    if (rideAvailability.availability > 0 && new Date(rideAvailability.nextReturnTime).getTime() > new Date().getTime()) {
      return rideAvailability.nextReturnTime;
    }

    try {
      const newNextReturnTime = addMinutesToISOString(rideAvailability.nextReturnTime, TIME_DURATION_MIN);

      await ddb.update({
        TableName: process.env.AVAILABLE_TICKETS_TABLE_NAME,
        Key: {
          rideId
        },
        ConditionExpression: `nextReturnTime = :NEXT_RETURN_TIME`,
        UpdateExpression: `SET nextReturnTime = :NEW_NEXT_RETURN_TIME, availability = :TICKETS`,
        ExpressionAttributeValues: {
          ':NEXT_RETURN_TIME': rideAvailability.nextReturnTime,
          ':NEW_NEXT_RETURN_TIME': newNextReturnTime,
          ':TICKETS': TICKETS_PER_TIME
        },
      }).promise();

      return newNextReturnTime;
    } catch (err) {
      // Ignore condition check failures, someone else already updated for more tickets
      if (err.code !== 'ConditionalCheckFailedException') {
        throw err;
      }
    }
  }
};

const createTicket = async (rideId, nextReturnTime) => {
  const ticketId = Math.floor(Math.random() * 1000000000).toString();

  await ddb.transactWrite({
    TransactItems: [
      {
        Update: {
          TableName: process.env.AVAILABLE_TICKETS_TABLE_NAME,
          Key: {
            rideId
          },
          UpdateExpression: 'ADD availability :MINUS_ONE',
          ConditionExpression: 'nextReturnTime = :NEXT_RETURN_TIME and availability > :ZERO',
          ExpressionAttributeValues: {
            ':MINUS_ONE': -1,
            ':ZERO': 0,
            ':NEXT_RETURN_TIME': nextReturnTime
          }
        }
      },
      {
        Put: {
          TableName: process.env.TICKETS_TABLE_NAME,
          Item: {
            ticketId,
            rideId,
            returnTime: nextReturnTime
          },
        }
      }
    ],

  }).promise();

  return ticketId;
}
