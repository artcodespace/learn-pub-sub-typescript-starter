import amqp from "amqplib";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import {
	AckType,
	SimpleQueueType,
	subscribeMsgPack,
} from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
	ExchangePerilDirect,
	ExchangePerilTopic,
	GameLogSlug,
	PauseKey,
} from "../internal/routing/routing.js";

async function main() {
	console.log("Starting Peril server...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
	const confChannel = await conn.createConfirmChannel();

	await subscribeMsgPack<GameLog>(
		conn,
		ExchangePerilTopic,
		GameLogSlug,
		`${GameLogSlug}.*`,
		SimpleQueueType.Durable,
		async (gameLog: GameLog) => {
			try {
				await writeLog(gameLog);
			} catch (err) {
				console.log(`Error writing log: ${(err as Error).message}`);
				process.stdout.write("> ");
				return AckType.NackRequeue;
			}
			process.stdout.write("> ");
			return AckType.Ack;
		},
	);
	console.log("Connection successful");
	process.on("SIGINT", async () => {
		console.log("Attempting shutdown");
		await conn.close();
		console.log("Shutdown complete");
		process.exit(0);
	});

	printServerHelp();
	while (true) {
		const input = await getInput();
		if (input.length === 0) continue;
		const first = input.at(0);
		if (first === "pause") {
			console.log("Sending a pause message");
			await publishJSON(confChannel, ExchangePerilDirect, PauseKey, {
				isPaused: true,
			});
		} else if (first === "resume") {
			console.log("Sending a resume message");
			await publishJSON(confChannel, ExchangePerilDirect, PauseKey, {
				isPaused: false,
			});
		} else if (first === "quit") {
			console.log("Exiting");
			break;
		} else {
			console.log("Unknown command");
		}
	}
}

main().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});
