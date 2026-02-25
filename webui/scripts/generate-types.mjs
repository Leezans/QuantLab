import { writeFileSync } from "node:fs";
import { resolve } from "node:path";

import openapiTS from "openapi-typescript";

const OPENAPI_URL = process.env.OPENAPI_URL ?? "http://127.0.0.1:8000/openapi.json";
const OUTPUT = resolve(process.cwd(), "src/types/generated/openapi.ts");

async function run() {
  const schema = await openapiTS(OPENAPI_URL);
  writeFileSync(OUTPUT, schema, "utf8");
  // eslint-disable-next-line no-console
  console.log(`Generated types from ${OPENAPI_URL} -> ${OUTPUT}`);
}

run().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
});
