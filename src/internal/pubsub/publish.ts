import { type ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
	ch: ConfirmChannel,
	exchange: string,
	routingKey: string,
	value: T,
): Promise<void> {
	const _v = Buffer.from(JSON.stringify(value));
	ch.publish(exchange, routingKey, _v, { contentType: "application/json" });
}

export async function publishMsgPack<T>(
	ch: ConfirmChannel,
	exchange: string,
	routingKey: string,
	value: T,
): Promise<void> {
	const _v = Buffer.from(encode(value));
	ch.publish(exchange, routingKey, _v, {
		contentType: "application/x-msgpack",
	});
}
