import amqp, { type Channel } from "amqplib";

const ExchangePerilDeadLetter = "peril_dlx";

export enum SimpleQueueType {
	Durable,
	Transient,
}

export enum AckType {
	Ack,
	NackRequeue,
	NackDiscard,
}
export async function declareAndBind(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	key: string,
	queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
	const channel = await conn.createChannel();
	const queue = await channel.assertQueue(queueName, {
		durable: queueType === SimpleQueueType.Durable,
		autoDelete: queueType === SimpleQueueType.Transient,
		exclusive: queueType === SimpleQueueType.Transient,
		arguments: {
			"x-dead-letter-exchange": ExchangePerilDeadLetter,
		},
	});
	await channel.bindQueue(queueName, exchange, key);
	return [channel, queue];
}

export async function subscribeJSON<T>(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	key: string,
	queueType: SimpleQueueType,
	handler: (data: T) => AckType,
): Promise<void> {
	const [channel, queue] = await declareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	);

	await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
		if (!message) {
			return;
		}
		const data = JSON.parse(message.content.toString()) as T;
		const ackType = handler(data);

		switch (ackType) {
			case AckType.Ack:
				channel.ack(message);
				console.log("Ack");
				break;
			case AckType.NackRequeue:
				channel.nack(message, false, true);
				console.log("NackRequeue");
				break;
			case AckType.NackDiscard:
				channel.nack(message, false, false);
				console.log("NackDiscard");
				break;
		}
	});
}
