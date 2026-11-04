import type { ConfirmChannel } from "amqplib";
import type {
	ArmyMove,
	RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishGameLog } from "./index.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
	ExchangePerilTopic,
	WarRecognitionsPrefix,
} from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
	return (ps: PlayingState) => {
		handlePause(gs, ps);
		process.stdout.write("> ");
		return AckType.Ack;
	};
}

export function handlerMove(
	gs: GameState,
	ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
	return async (move: ArmyMove) => {
		const outcome = handleMove(gs, move);

		switch (outcome) {
			case MoveOutcome.Safe:
				process.stdout.write("> ");
				return AckType.Ack;
			case MoveOutcome.MakeWar: {
				const rw: RecognitionOfWar = {
					attacker: move.player,
					defender: gs.getPlayerSnap(),
				};
				try {
					await publishJSON(
						ch,
						ExchangePerilTopic,
						`${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
						rw,
					);
				} catch (err) {
					console.log(
						`Error publishing war declaration: ${(err as Error).message}`,
					);
					process.stdout.write("> ");
					return AckType.NackRequeue;
				}
				process.stdout.write("> ");
				return AckType.Ack;
			}
			case MoveOutcome.SamePlayer:
				process.stdout.write("> ");
				return AckType.NackDiscard;
			default:
				process.stdout.write("> ");
				return AckType.NackDiscard;
		}
	};
}

export function handlerWar(
	gs: GameState,
	ch: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
	return async (rw: RecognitionOfWar) => {
		const resolution = handleWar(gs, rw);
		process.stdout.write("> ");

		let message: string;
		switch (resolution.result) {
			case WarOutcome.NotInvolved:
				return AckType.NackRequeue;
			case WarOutcome.NoUnits:
				return AckType.NackDiscard;
			case WarOutcome.OpponentWon:
			case WarOutcome.YouWon:
				message = `${resolution.winner} won a war against ${resolution.loser}`;
				break;
			case WarOutcome.Draw:
				message = `A war between ${resolution.attacker} and ${resolution.defender} resulted in a draw`;
				break;
			default:
				console.log("Error: unknown war outcome");
				return AckType.NackDiscard;
		}

		try {
			await publishGameLog(ch, rw.attacker.username, message);
		} catch (err) {
			console.log(`Error publishing game log: ${(err as Error).message}`);
			return AckType.NackRequeue;
		}
		return AckType.Ack;
	};
}
