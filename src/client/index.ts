import amqp, { type ConfirmChannel } from "amqplib";
import {
	clientWelcome,
	commandStatus,
	getInput,
	printClientHelp,
	printQuit,
} from "../internal/gamelogic/gamelogic.js";
import type {
	ArmyMove,
	RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import {
	ArmyMovesPrefix,
	ExchangePerilDirect,
	ExchangePerilTopic,
	GameLogSlug,
	PauseKey,
	WarRecognitionsPrefix,
} from "../internal/routing/routing.js";

export async function publishGameLog(
	ch: ConfirmChannel,
	username: string,
	message: string,
): Promise<void> {
	const gameLog: GameLog = {
		username,
		message,
		currentTime: new Date(),
	};
	await publishMsgPack(
		ch,
		ExchangePerilTopic,
		`${GameLogSlug}.${username}`,
		gameLog,
	);
}

async function main() {
	console.log("Starting Peril client...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const clientName = await clientWelcome();
	const conn = await amqp.connect(connectionString);
	const confChannel = await conn.createConfirmChannel();

	console.log("Connection successful");

	process.on("SIGINT", async () => {
		console.log("Attempting shutdown");
		await conn.close();
		console.log("Shutdown complete");
		process.exit(0);
	});

	const gameState = new GameState(clientName);

	await subscribeJSON<PlayingState>(
		conn,
		ExchangePerilDirect,
		`${PauseKey}.${clientName}`,
		PauseKey,
		SimpleQueueType.Transient,
		handlerPause(gameState),
	);

	await subscribeJSON<ArmyMove>(
		conn,
		ExchangePerilTopic,
		`${ArmyMovesPrefix}.${clientName}`,
		`${ArmyMovesPrefix}.*`,
		SimpleQueueType.Transient,
		handlerMove(gameState, confChannel),
	);

	await subscribeJSON<RecognitionOfWar>(
		conn,
		ExchangePerilTopic,
		WarRecognitionsPrefix,
		`${WarRecognitionsPrefix}.*`,
		SimpleQueueType.Durable,
		handlerWar(gameState, confChannel),
	);

	while (true) {
		const words = await getInput();
		if (!words[0]) {
			continue;
		}

		if (words[0] === "spawn") {
			try {
				commandSpawn(gameState, words);
			} catch (err) {
				console.log((err as Error).message);
			}
		} else if (words[0] === "move") {
			try {
				const move = commandMove(gameState, words);
				await publishJSON(
					confChannel,
					ExchangePerilTopic,
					`${ArmyMovesPrefix}.${clientName}`,
					move,
				);
				console.log("Move published successfully");
			} catch (err) {
				console.log((err as Error).message);
			}
		} else if (words[0] === "status") {
			await commandStatus(gameState);
		} else if (words[0] === "help") {
			printClientHelp();
		} else if (words[0] === "spam") {
			console.log("Spamming not allowed yet!");
		} else if (words[0] === "quit") {
			printQuit();
			break;
		} else {
			console.log(`Unknown command: ${words[0]}`);
		}
	}

	await conn.close();
}

main().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});
