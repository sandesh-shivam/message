import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { Server } from 'socket.io';
import { io as createClient } from 'socket.io-client';

// Keep the test isolated from any developer machine / CI PostgreSQL configuration.
// The repository container is selected when repositories/index.js is imported below.
delete process.env.DATABASE_URL;

const [{ setupSocketHandlers }, { userRepository }] = await Promise.all([
  import('../sockets/socketHandler.js'),
  import('../repositories/index.js')
]);

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForStatus(userId, expectedOnline, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const user = await userRepository.findById(userId);
    if (user && user.isOnline === expectedOnline) return user;
    await wait(10);
  }
  const user = await userRepository.findById(userId);
  assert.equal(user?.isOnline, expectedOnline, `user ${userId} did not reach isOnline=${expectedOnline}`);
  return user;
}

async function createUser(label) {
  return userRepository.create({
    id: `presence-test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    username: `presence_${label}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
    displayName: `Presence Test ${label}`,
    passwordHash: 'test-only'
  });
}

async function createHarness() {
  const httpServer = http.createServer();
  const ioServer = new Server(httpServer, {
    pingInterval: 1000,
    pingTimeout: 1000
  });

  ioServer.use((socket, next) => {
    const id = socket.handshake.auth?.userId;
    if (!id) return next(new Error('test user id missing'));
    socket.user = {
      id,
      username: String(id),
      displayName: String(id),
      avatarUrl: ''
    };
    next();
  });

  setupSocketHandlers(ioServer);
  await new Promise((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(0, '127.0.0.1', resolve);
  });

  const address = httpServer.address();
  const url = `http://127.0.0.1:${address.port}`;
  const clients = [];

  return {
    client(userId) {
      const socket = createClient(url, {
        auth: { userId },
        autoConnect: false,
        reconnection: false,
        transports: ['websocket']
      });
      clients.push(socket);
      return socket;
    },
    async connect(socket) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('socket connect timed out')), 3000);
        socket.once('connect', () => {
          clearTimeout(timer);
          resolve();
        });
        socket.once('connect_error', (error) => {
          clearTimeout(timer);
          reject(error);
        });
        socket.connect();
      });
    },
    async close() {
      for (const socket of clients) socket.disconnect();
      await new Promise((resolve) => ioServer.close(resolve));
    }
  };
}

test('presence stays online until the last tab disconnects', async () => {
  const user = await createUser('multitab');
  const harness = await createHarness();

  try {
    const firstTab = harness.client(user.id);
    const secondTab = harness.client(user.id);

    await harness.connect(firstTab);
    await waitForStatus(user.id, true);
    await harness.connect(secondTab);
    await waitForStatus(user.id, true);

    firstTab.disconnect();
    await wait(100);
    assert.equal((await userRepository.findById(user.id)).isOnline, true,
      'disconnecting one tab must not mark the other active tab offline');

    secondTab.disconnect();
    await waitForStatus(user.id, false);
  } finally {
    await harness.close();
  }
});

test('a rapid connect/disconnect does not leave a stale online flag', async () => {
  const user = await createUser('rapid');
  const observerUser = await createUser('observer');
  const harness = await createHarness();

  try {
    const observer = harness.client(observerUser.id);
    await harness.connect(observer);

    const wentOffline = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('offline presence event timed out')), 3000);
      observer.on('user_presence_change', (event) => {
        if (event.userId === user.id && event.isOnline === false) {
          clearTimeout(timer);
          resolve();
        }
      });
    });

    const target = harness.client(user.id);
    const initialPresence = new Promise((resolve) => target.once('initial_presence_state', resolve));
    await harness.connect(target);
    await initialPresence;

    // Disconnect as soon as the server has accepted the connection.
    target.disconnect();

    await wentOffline;
    await waitForStatus(user.id, false);
  } finally {
    await harness.close();
  }
});
