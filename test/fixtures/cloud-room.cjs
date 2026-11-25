#!/usr/bin/env node
'use strict';

// From a backend with @dtelecom/server-sdk-js installed, run
//   node --env-file=.env.local /absolute/path/to/test/fixtures/cloud-room.cjs /absolute/private/output
// The existing output directory receives a fresh room's five JWTs. The backend env supplies
// DTELECOM_CLOUD_URL, DTELECOM_API_KEY and DTELECOM_API_SECRET. When invoking through Mage
// from the server checkout, export those variables and DTELECOM_FIXTURE_BACKEND_DIR=/path/to/backend.
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createRequire } = require('node:module');

async function issueRoom(directory) {
  if (!['DTELECOM_CLOUD_URL', 'DTELECOM_API_KEY', 'DTELECOM_API_SECRET'].every(key => process.env[key])) {
    throw new Error('Set DTELECOM_CLOUD_URL, DTELECOM_API_KEY and DTELECOM_API_SECRET for the test project.');
  }
  const backend = process.env.DTELECOM_FIXTURE_BACKEND_DIR || process.cwd();
  let client;
  try {
    const backendRequire = createRequire(path.resolve(backend, 'package.json'));
    const { CloudClient } = backendRequire('@dtelecom/server-sdk-js');
    client = new CloudClient();
  } catch {
    throw new Error('Could not load CloudClient from the backend. Check its SDK installation and Node version.');
  }

  const room = 'sfu-test-' + randomUUID();
  const created = [];
  let errorMessage = 'Provide an existing writable directory with no conflicting JWT files.';
  try {
    // Reserve every output before asking Cloud for sessions. Never replace an existing token.
    for (const label of ['a', 'b', 'c', 'restricted', 'admin']) {
      const filename = path.join(directory, label + '.jwt');
      const fd = fs.openSync(filename, 'wx', 0o600);
      created.push({ label, filename, fd });
    }

    errorMessage = 'Could not issue or write the room tokens. Check the test project credentials, Cloud URL and balance.';
    for (const { label, fd } of created) {
      const result = label === 'admin' ? await client.adminToken(room) : await client.createToken({
        room,
        identity: 'fixture-' + label,
        name: 'Fixture ' + label,
        metadata: JSON.stringify({ fixture: label }),
        ttl: '5m',
        grants: { canPublish: true, canPublishData: true, canSubscribe: label !== 'restricted' },
      });
      if (typeof result.token !== 'string' || !result.token) throw new Error();
      fs.writeFileSync(fd, result.token + '\n');
    }
  } catch {
    for (const { filename } of created) fs.unlinkSync(filename);
    throw new Error(errorMessage);
  } finally {
    for (const { fd } of created) fs.closeSync(fd);
  }
}

const [directory] = process.argv.slice(2);
if (process.argv.length !== 3 || !path.isAbsolute(directory)) {
  console.error('Usage: cloud-room.cjs /absolute/private/output');
  process.exitCode = 2;
} else {
  issueRoom(directory).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
