/*
 * This file is part of a proprietary work.
 *
 * Copyright (c) 2025-2026 Fossorial, Inc.
 * All rights reserved.
 *
 * This file is licensed under the Fossorial Commercial License.
 * You may not use this file except in compliance with the License.
 * Unauthorized use, copying, modification, or distribution is strictly prohibited.
 *
 * This file is not licensed under the AGPLv3.
 */

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { assertEquals } from "@test/assert";
import { LogStreamingManager } from "./LogStreamingManager";
import config from "@server/lib/config";
import { encrypt } from "@server/lib/crypto";
import { LogDeliveryError } from "./providers/LogDestinationProvider";
import { parseRetryAfterMs } from "./providers/HttpLogDestination";

// Endpoint that accepts every request except the `failOn`-th (0-based) and
// records the event ids it accepted.
async function withEndpoint(
    failOn: number,
    run: (url: string, received: number[]) => Promise<void>
): Promise<void> {
    const received: number[] = [];
    let n = 0;
    const server = createServer((req, res) => {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
            if (n++ === failOn) {
                res.writeHead(429);
            } else {
                received.push(JSON.parse(raw).data.id);
                res.writeHead(204);
            }
            res.end();
        });
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const { port } = server.address() as AddressInfo;
    try {
        await run(`http://127.0.0.1:${port}/`, received);
    } finally {
        server.close();
    }
}

// A manager whose cursor, log rows and destination status live in memory.
// Only these DB helpers are replaced; the poll logic under test runs as is.
function manager(rowCount: number) {
    const rows = Array.from({ length: rowCount }, (_, i) => ({
        id: i + 1,
        orgId: "org1",
        timestamp: 1_700_000_000
    }));
    const state = { cursor: 0 };
    const m = new LogStreamingManager() as any;
    m.isRunning = true;
    m.getOrCreateCursor = async () => ({ lastSentId: state.cursor });
    m.fetchLogs = async (_t: string, _o: string, afterId: number) =>
        rows.filter((r) => r.id > afterId);
    m.updateCursor = async (_d: number, _t: string, id: number) => {
        state.cursor = id;
    };
    m.setDestinationError = async () => {};
    m.clearDestinationError = async () => {};
    return { m, state };
}

function destination(url: string) {
    const http = {
        name: "hook",
        url,
        authType: "none",
        headers: [],
        format: "json_single",
        useBodyTemplate: false
    };
    return {
        destinationId: 1,
        orgId: "org1",
        type: "http",
        sendActionLogs: true,
        config: encrypt(
            JSON.stringify(http),
            config.getRawConfig().server.secret!
        )
    };
}

async function testPartialDeliveryMovesTheCursor() {
    console.log("Running partial delivery tests...");
    // [request that fails, cursor after the failed poll, backing off]
    const cases: [number, number, boolean][] = [
        [3, 3, false], // events 1-3 accepted: resume at 4, no back-off
        [0, 0, true] // nothing accepted: back off as before
    ];
    for (const [failOn, cursor, backingOff] of cases) {
        await withEndpoint(failOn, async (url, received) => {
            const { m, state } = manager(6);
            const dest = destination(url);

            await m.processDestination(dest);
            assertEquals(state.cursor, cursor, `cursor (fail on ${failOn})`);
            assertEquals(
                m.failures.has(1),
                backingOff,
                `back-off (fail on ${failOn})`
            );

            m.failures.clear();
            await m.processDestination(dest);
            assertEquals(state.cursor, 6, "retry finishes the batch");
            assertEquals(received.join(","), "1,2,3,4,5,6", "each event once");
        });
    }

    // One log type progresses while another fails outright: still back off.
    {
        const { m } = manager(2);
        m.processLogType = async (
            _d: unknown,
            _p: unknown,
            logType: string
        ) => {
            throw logType === "action"
                ? new LogDeliveryError("slow", 1)
                : new Error("rejected");
        };
        await m.processDestination({
            ...destination("http://127.0.0.1:9/"),
            sendAccessLogs: true
        });
        assertEquals(m.failures.has(1), true, "a stalled log type backs off");
    }

    // A provider that over-reports never moves the cursor past the batch.
    const { m, state } = manager(2);
    const overReporting = {
        type: "test",
        send: async () => {
            throw new LogDeliveryError("boom", 5);
        }
    };
    await m
        .processLogType(
            { destinationId: 1, orgId: "org1" },
            overReporting,
            "action"
        )
        .catch(() => {});
    assertEquals(state.cursor, 2, "cursor stops at the last row of the batch");
}

// Like a Discord webhook: `limit` requests per window, then 429 with the
// Retry-After header from `retryAfter(secondsLeftInWindow)`.
async function withRateLimit(
    limit: number,
    windowMs: number,
    retryAfter: (secondsLeft: number) => string,
    run: (
        url: string,
        received: number[],
        requests: () => number
    ) => Promise<void>
): Promise<void> {
    const received: number[] = [];
    let requests = 0;
    let start = 0;
    let used = 0;
    const server = createServer((req, res) => {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
            requests++;
            const now = Date.now();
            if (now - start >= windowMs) {
                start = now;
                used = 0;
            }
            if (used >= limit) {
                const left = (windowMs - (now - start)) / 1000;
                res.writeHead(429, { "Retry-After": retryAfter(left) });
            } else {
                used++;
                received.push(JSON.parse(raw).data.id);
                res.writeHead(204);
            }
            res.end();
        });
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const { port } = server.address() as AddressInfo;
    try {
        await run(`http://127.0.0.1:${port}/`, received, () => requests);
    } finally {
        server.close();
    }
}

async function testRetryAfterIsWaitedOut() {
    console.log("Running Retry-After tests...");
    await withRateLimit(5, 300, String, async (url, received, requests) => {
        const { m, state } = manager(12);
        await m.processDestination(destination(url));
        assertEquals(state.cursor, 12, "whole batch delivered in one poll");
        assertEquals(m.failures.has(1), false, "no back-off");
        assertEquals(
            received.join(","),
            "1,2,3,4,5,6,7,8,9,10,11,12",
            "once each, in order"
        );
        assertEquals(requests() > 12, true, "the endpoint pushed back");
    });
    await withRateLimit(
        1,
        60_000,
        () => "60",
        async (url, _r, requests) => {
            const { m, state } = manager(3);
            const started = Date.now();
            await m.processDestination(destination(url));
            assertEquals(
                Date.now() - started < 1000,
                true,
                "a wait past the budget is not taken"
            );
            assertEquals(state.cursor, 1, "cursor keeps the delivered event");
            assertEquals(requests(), 2, "no retry");
        }
    );

    assertEquals(parseRetryAfterMs("1.5"), 1500, "seconds");
    assertEquals(parseRetryAfterMs(null), null, "missing");
    assertEquals(parseRetryAfterMs("soon"), null, "unparseable");
    assertEquals(
        parseRetryAfterMs(new Date(Date.now() - 1000).toUTCString()),
        0,
        "HTTP date in the past"
    );
}

testPartialDeliveryMovesTheCursor()
    .then(testRetryAfterIsWaitedOut)
    .then(() => console.log("All LogStreamingManager tests passed"))
    .catch((err) => {
        console.error(err);
        process.exit(1);
    });
