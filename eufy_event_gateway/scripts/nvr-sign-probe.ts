/**
 * Research probe for RTC-only NVR stations (T8N00, Mega type 300).
 *
 * Question: does the smart-service signalling endpoint issue an NVR session
 * token to the gateway's existing Mega session? The public web client obtains
 * this token with its own portal login; if the Mega session is accepted, the
 * gateway can open NVR signalling without any additional login path.
 *
 * Prints only model, result codes, and token lengths. Never prints the token,
 * account details, or full serial numbers.
 */
import { createInterface } from "node:readline/promises";
import { MegaClient } from "../src/mega/client.js";
import { parseMegaInventory } from "../src/provider/eufy-provider.js";

const username = process.env.EUFY_USERNAME;
const password = process.env.EUFY_PASSWORD;
const country = process.env.EUFY_COUNTRY ?? "GB";
const region = (process.env.EUFY_SMART_REGION ?? "eu") as "us" | "eu" | "ie";
const dataDirectory = process.env.EUFY_GATEWAY_DATA_DIR ?? "./data";
if (!username || !password) throw new Error("EUFY_USERNAME and EUFY_PASSWORD are required (values are never printed)");

const client = new MegaClient({ email: username, password, country, persistentDirectory: dataDirectory });

/** Complete an email-verification challenge in this process, so no second client competes for the session. */
async function completeVerification(state: string): Promise<string> {
  if (state !== "verification-required") return state;
  const prompt = createInterface({ input: process.stdin, output: process.stdout });
  const code = (await prompt.question("Eufy emailed a verification code. Enter it: ")).trim();
  prompt.close();
  return (await client.connect(code)).state;
}

let auth = await client.connect();
auth = { ...auth, state: await completeVerification(auth.state) } as typeof auth;
if (auth.state !== "authenticated") throw new Error(`Mega authentication is ${auth.state}; complete it in the gateway Web UI first`);

let inventory;
try {
  inventory = await client.inventory();
} catch (error) {
  // Same recovery as ppcs-probe: a rejected saved session is re-established once.
  if (!client.isSessionInvalidError(error) && !/\(401:/.test(String(error))) throw new Error("Mega inventory request failed (details withheld)");
  auth = await client.connect(undefined, undefined, true);
  auth = { ...auth, state: await completeVerification(auth.state) } as typeof auth;
  if (auth.state !== "authenticated") throw new Error(`Mega re-authentication is ${auth.state}; complete it in the gateway Web UI first`);
  inventory = await client.inventory();
}
const devices = parseMegaInventory(inventory);
const nvrs = devices.filter((device) => device.model.toUpperCase().startsWith("T8N00"));
if (nvrs.length === 0) throw new Error("No T8N00 station in this account's inventory");

for (const nvr of nvrs) {
  for (const probeRegion of ["eu", "us", "ie"] as const) for (const modelType of ["PHONE", "WEB"]) {
    try {
      const result = await client.probeNvrSignToken(nvr.serial, modelType, probeRegion);
      console.log(JSON.stringify({ model: nvr.model, serialTail: nvr.serial.slice(-4), modelType, region: probeRegion, ...result }));
    } catch (error) {
      console.log(JSON.stringify({ model: nvr.model, modelType, region: probeRegion, error: error instanceof Error ? error.message : String(error) }));
    }
  }
}
